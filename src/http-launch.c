/*
 * Copyright (C) 2013 Sebastian Dröge <slomo@circular-chaos.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 51 Franklin St, Fifth Floor,
 * Boston, MA 02110-1301, USA.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <gst/gst.h>
#include <gio/gio.h>

typedef struct
{
  GstElement *element;
  gchar *name;
  gchar *content_type;
  gboolean caps_resolved;
} EndPoint;

typedef struct
{
  gchar *name;
  GSocketConnection *connection;
  GSocket *socket;
  GInputStream *istream;
  GOutputStream *ostream;
  GSource *isource, *tosource;
  GByteArray *current_message;
  gchar *http_version;
  gboolean waiting_200_ok;
  EndPoint *endpoint;
} Client;

static GMainLoop *loop = NULL;
G_LOCK_DEFINE_STATIC (clients);
static GList *clients = NULL;
static GList *endpoints = NULL;
static GstElement *pipeline = NULL;
static gboolean started = FALSE;
G_LOCK_DEFINE_STATIC (caps);

/* Maximum in memory buffer duration for flashback */
#define MAX_FLASHBACK 120
/* Configure pipeline here, just name the multisocketsink, don't configure them */
#define PIPELINE_DESC "videotestsrc is-live=true ! video/x-raw, width=640, height=480 ! timeoverlay ! x264enc key-int-max=30 b-adapt=0 ! h264parse ! queue ! matroskamux streamable=true ! multisocketsink name=test"

/* Helper to find regions by id */
static gint
endpoint_compare_by_name (EndPoint * endpoint, gchar * name)
{
  return g_strcmp0 (endpoint->name, name);
}

static gint
endpoint_compare_by_element (EndPoint * endpoint, GstElement * element)
{
  return endpoint->element != element;
}

static void
remove_client (Client * client)
{
  g_print ("Removing connection %s\n", client->name);

  G_LOCK (clients);
  clients = g_list_remove (clients, client);
  G_UNLOCK (clients);

  g_free (client->name);
  g_free (client->http_version);

  if (client->isource) {
    g_source_destroy (client->isource);
    g_source_unref (client->isource);
  }
  if (client->tosource) {
    g_source_destroy (client->tosource);
    g_source_unref (client->tosource);
  }
  g_object_unref (client->connection);
  g_byte_array_unref (client->current_message);

  g_slice_free (Client, client);
}

static void
write_bytes (Client * client, const gchar * data, guint len)
{
  gssize w;
  GError *err = NULL;

  /* TODO: We assume this never blocks */
  do {
    w = g_output_stream_write (client->ostream, data, len, NULL, &err);
    if (w > 0) {
      len -= w;
      data += w;
    }
  } while (w > 0 && len > 0);

  if (w <= 0) {
    if (err) {
      g_print ("Write error %s\n", err->message);
      g_clear_error (&err);
    }
    remove_client (client);
  }
}

static void
send_response_200_ok (Client * client)
{
  gchar *response = g_strdup_printf ("%s 200 OK\r\n%s\r\n", client->http_version,
        client->endpoint->content_type);
  write_bytes (client, response, strlen (response));
  g_free (response);
}

static void
send_response_404_not_found (Client * client)
{
  gchar *response;
  g_print ("sending 404 error\n");
  response = g_strdup_printf ("%s 404 Not Found\r\n\r\n", client->http_version);
  write_bytes (client, response, strlen (response));
  g_free (response);

}

static void
send_response_400_not_found (Client * client)
{
  gchar *response;
  g_print ("sending 400 error\n");
  response = g_strdup_printf ("%s 400 Bad Request\r\n\r\n", client->http_version);
  write_bytes (client, response, strlen (response));
  g_free (response);
}

static gboolean
client_message (Client * client, const gchar * data, guint len)
{
  gboolean http_head_request = FALSE;
  gboolean http_get_request = FALSE;
  gboolean ret = FALSE;
  gint burst_mode = 2;
  guint64 min_value = -1, max_value = -1;
  gchar **lines = g_strsplit_set (data, "\r\n", -1);

  if (g_str_has_prefix (lines[0], "HEAD"))
    http_head_request = TRUE;
  else if (g_str_has_prefix (lines[0], "GET"))
    http_get_request = TRUE;

  if (http_head_request || http_get_request) {
    gchar **parts = g_strsplit (lines[0], " ", -1);

    g_free (client->http_version);

    if (parts[1] && parts[2] && *parts[2] != '\0')
      client->http_version = g_strdup (parts[2]);
    else
      client->http_version = g_strdup ("HTTP/1.0");

    g_print ("request : %s\n", parts[1]);

    if (parts[1]) {
      gchar **path_parts = g_strsplit (parts[1], "/", -1);
      gint nb_parts = g_strv_length (path_parts);

      g_print ("parts in request %d\n", nb_parts);

      if (nb_parts > 1) {
        /* Try searching for endpoint with first part */
        GList *result = g_list_find_custom (endpoints, path_parts[1], (GCompareFunc) endpoint_compare_by_name);

        if (result) {
          g_print ("found endpoint %p for request part %s\n", result->data, path_parts[1]);
          client->endpoint = (EndPoint *) result->data;

          G_LOCK (caps);
          if (client->endpoint->caps_resolved)
            send_response_200_ok (client);
          else
            client->waiting_200_ok = TRUE;
          G_UNLOCK (caps);
          ret = TRUE;
        } else {
          /* That s a 404 ! */
          g_print ("no endpoint found for request part %s\n", path_parts[1]);
          send_response_404_not_found (client);
        }
      }

      /* Depending on the second part we add at the end of buffer or in the past */
      if (nb_parts > 2 && ret) {
        if (!g_strcmp0 (path_parts[2], "flashback")) {
          g_print ("adding client using flashback mode\n");
          burst_mode = 4;
          min_value = 30 * GST_SECOND;
        } else if (!g_strcmp0 (path_parts[2], "feedback")) {
          g_print ("adding client using feedback mode\n");
        }
      }

      if (nb_parts > 3 && ret) {
        const gint offset = (gint) g_ascii_strtoll (path_parts[3], NULL, 10);

        if (offset > 0 && offset < MAX_FLASHBACK) {
          g_print ("configure flashback offset to %d seconds\n", offset);
          min_value = offset * GST_SECOND;
        }
      }

      g_strfreev (path_parts);
    }

    g_strfreev (parts);

    if (ret) {
      if (http_get_request) {
        /* Start streaming to client socket */
        g_source_destroy (client->isource);
        g_source_unref (client->isource);
        client->isource = NULL;
        g_source_destroy (client->tosource);
        g_source_unref (client->tosource);
        client->tosource = NULL;
        g_print ("Starting to stream to %s\n", client->name);
        g_signal_emit_by_name (client->endpoint->element, "add-full", client->socket,
            burst_mode, GST_FORMAT_TIME, min_value, GST_FORMAT_TIME, max_value);
      }

      if (!started) {
        g_print ("Starting pipeline\n");
        if (gst_element_set_state (pipeline,
                GST_STATE_PLAYING) == GST_STATE_CHANGE_FAILURE) {
          g_print ("Failed to start pipeline\n");
          g_main_loop_quit (loop);
        }
        started = TRUE;
      }
    }
  } else {
    send_response_400_not_found (client);
  }

  g_strfreev (lines);

  return ret;
}

static gboolean
on_timeout (Client * client)
{
  g_print ("Timeout\n");
  remove_client (client);

  return FALSE;
}

static gboolean
on_read_bytes (GPollableInputStream * stream, Client * client)
{
  gssize r;
  gchar data[4096];
  GError *err = NULL;

  do {
    r = g_pollable_input_stream_read_nonblocking (G_POLLABLE_INPUT_STREAM
        (client->istream), data, sizeof (data), NULL, &err);
    if (r > 0)
      g_byte_array_append (client->current_message, (guint8 *) data, r);
  } while (r > 0);

  if (r == 0) {
    remove_client (client);
    return FALSE;
  } else if (g_error_matches (err, G_IO_ERROR, G_IO_ERROR_WOULD_BLOCK)) {
    guint8 *tmp = client->current_message->data;
    guint tmp_len = client->current_message->len;

    g_clear_error (&err);

    while (tmp_len > 3) {
      if (tmp[0] == 0x0d && tmp[1] == 0x0a && tmp[2] == 0x0d && tmp[3] == 0x0a) {
        guint len;

        g_byte_array_append (client->current_message, (const guint8 *) "\0", 1);
        len = tmp - client->current_message->data + 5;
        if (client_message (client, (gchar *) client->current_message->data, len)) {
          g_byte_array_remove_range (client->current_message, 0, len);
          tmp = client->current_message->data;
  	      tmp_len = client->current_message->len;
        }
        else {
          g_print ("client message processing generated an error\n");
          remove_client (client);
        }
      } else {
        tmp++;
	      tmp_len--;
      }
    }

    if (client->current_message->len >= 1024 * 1024) {
      g_print ("No complete request after 1MB of data\n");
      remove_client (client);
      return FALSE;
    }

    return TRUE;
  } else {
    g_print ("Read error %s\n", err->message);
    g_clear_error (&err);
    remove_client (client);
    return FALSE;
  }

  return FALSE;
}

static gboolean
on_new_connection (GSocketService * service, GSocketConnection * connection,
    GObject * source_object, gpointer user_data)
{
  Client *client = g_slice_new0 (Client);
  GSocketAddress *addr;
  GInetAddress *iaddr;
  gchar *ip;
  guint16 port;

  addr = g_socket_connection_get_remote_address (connection, NULL);
  iaddr = g_inet_socket_address_get_address (G_INET_SOCKET_ADDRESS (addr));
  port = g_inet_socket_address_get_port (G_INET_SOCKET_ADDRESS (addr));
  ip = g_inet_address_to_string (iaddr);
  client->name = g_strdup_printf ("%s:%u", ip, port);
  g_free (ip);
  g_object_unref (addr);

  g_print ("New connection %s\n", client->name);

  client->waiting_200_ok = FALSE;
  client->http_version = g_strdup ("");
  client->connection = g_object_ref (connection);
  client->socket = g_socket_connection_get_socket (connection);
  client->istream =
      g_io_stream_get_input_stream (G_IO_STREAM (client->connection));
  client->ostream =
      g_io_stream_get_output_stream (G_IO_STREAM (client->connection));
  client->current_message = g_byte_array_sized_new (1024);

  client->tosource = g_timeout_source_new_seconds (5);
  g_source_set_callback (client->tosource, (GSourceFunc) on_timeout, client,
      NULL);
  g_source_attach (client->tosource, NULL);

  client->isource =
      g_pollable_input_stream_create_source (G_POLLABLE_INPUT_STREAM
      (client->istream), NULL);
  g_source_set_callback (client->isource, (GSourceFunc) on_read_bytes, client,
      NULL);
  g_source_attach (client->isource, NULL);

  G_LOCK (clients);
  clients = g_list_prepend (clients, client);
  G_UNLOCK (clients);

  return TRUE;
}

static gboolean
on_message (GstBus * bus, GstMessage * message, gpointer user_data)
{
  switch (GST_MESSAGE_TYPE (message)) {
    case GST_MESSAGE_ERROR:{
      gchar *debug;
      GError *err;

      gst_message_parse_error (message, &err, &debug);
      g_print ("Error %s\n", err->message);
      g_error_free (err);
      g_free (debug);
      g_main_loop_quit (loop);
      break;
    }
    case GST_MESSAGE_WARNING:{
      gchar *debug;
      GError *err;

      gst_message_parse_warning (message, &err, &debug);
      g_print ("Warning %s\n", err->message);
      g_error_free (err);
      g_free (debug);
      break;
    }
    case GST_MESSAGE_EOS:{
      g_print ("EOS\n");
      g_main_loop_quit (loop);
    }
    default:
      break;
  }

  return TRUE;
}

static void
on_client_socket_removed (GstElement * element, GSocket * socket,
    gpointer user_data)
{
  GList *l;
  Client *client = NULL;

  g_print ("client socket removed\n");

  G_LOCK (clients);
  for (l = clients; l; l = l->next) {
    Client *tmp = l->data;
    if (socket == tmp->socket) {
      client = tmp;
      break;
    }
  }
  G_UNLOCK (clients);

  if (client)
    remove_client (client);
}

/* Identify the endpoint which configured caps, and notify clients
 * connected to it waiting for content_type */
static void on_stream_caps_changed (GObject *obj, GParamSpec *pspec,
    gpointer user_data)
{
  GList *l;
  GstElement *element = gst_pad_get_parent_element (GST_PAD (obj));

  l = g_list_find_custom (endpoints, element, (GCompareFunc) endpoint_compare_by_element);

  if (G_LIKELY (l)) {
    EndPoint *endpoint = (EndPoint *) l->data;
    GstPad *src_pad = (GstPad *) obj;
    GstCaps *src_caps = gst_pad_get_current_caps (src_pad);
    GstStructure *s = gst_caps_get_structure (src_caps, 0);

    endpoint->content_type = g_strdup_printf ("Content-Type: %s\r\n", gst_structure_get_name (s));
    endpoint->caps_resolved = TRUE;

    g_print ("found content type %s for endpoint %s\n", gst_structure_get_name (s), endpoint->name);

    gst_caps_unref (src_caps);
  }

  /* Send 200 OK to those clients waiting for it */
  G_LOCK (caps);

  G_LOCK (clients);
  for (l = clients; l; l = l->next) {
    Client *cl = l->data;
    if (cl->endpoint->element == element && cl->waiting_200_ok) {
      send_response_200_ok (cl);
      cl->waiting_200_ok = FALSE;
      break;
    }
  }
  G_UNLOCK (clients);

  G_UNLOCK (caps);

  gst_object_unref (element);
}

int
main (gint argc, gchar ** argv)
{
  GSocketService *service;
  GError *err = NULL;
  GstBus *bus;
  GstIterator *it;

  /*if (daemon (0, 0) == -1) {
    g_print ("Cannot detach!\n");
    return -1;
  }
  else {
    FILE *pid_file = NULL;

    pid_file = fopen ("/var/run/http-launch.pid", "a+");
    fprintf (pid_file, "%d\n", getpid ());
    fclose (pid_file);
  }*/

  gst_init (&argc, &argv);

  if (argc < 2) {
    g_print ("usage: %s PORT\n"
        "example: %s 8080\n",
        argv[0], argv[0]);
    return -1;
  }

  const gchar *port_str = argv[1];
  const int port = (int) g_ascii_strtoll(port_str, NULL, 10);

  pipeline = gst_parse_launch (PIPELINE_DESC, &err);
  if (!pipeline) {
    g_print ("invalid pipeline: %s\n", err->message);
    g_clear_error (&err);
    return -2;
  }

  /* Find all multisocketsink elements */
  GValue it_value = G_VALUE_INIT;
  it = gst_bin_iterate_elements (GST_BIN (pipeline));
  while (gst_iterator_next (it, &it_value) != GST_ITERATOR_DONE) {
    GstElementFactory *elm_fact;
    GstElement *elm = (GstElement *) g_value_get_object (&it_value);

    /* Check factory name */
    elm_fact = gst_element_get_factory (elm);

    if (!g_ascii_strcasecmp ("multisocketsink", gst_plugin_feature_get_name (elm_fact))) {
      EndPoint *endpoint = g_new0 (EndPoint, 1);
      GstPad *pad = gst_element_get_static_pad (elm, "sink");

      g_print ("Found endpoint named %s\n", GST_OBJECT_NAME (elm));

      g_object_set (elm,
        "unit-format", GST_FORMAT_TIME,
        "units-max", (gint64) 7 * GST_SECOND, /* Slow clients get dropped when they get that late */
        "units-soft-max", (gint64) 3 * GST_SECOND, /* Recovery procedure starts */
        "recover-policy", 3 /* keyframe */ ,
        "timeout", (guint64) 10 * GST_SECOND,
        "time-min", (gint64) MAX_FLASHBACK * GST_SECOND, /* Keep 2 minutes in memory */
        "sync-method", 2 /* latest-keyframe */ ,
        NULL);

      g_signal_connect (pad, "notify::caps", G_CALLBACK (on_stream_caps_changed), NULL);
      g_signal_connect (elm, "client-socket-removed", G_CALLBACK (on_client_socket_removed), NULL);

      endpoint->element = gst_object_ref (elm);
      endpoint->name = gst_element_get_name (elm);

      endpoints = g_list_append (endpoints, endpoint);

      gst_object_unref (pad);
    }

    g_value_reset (&it_value);
  }
  gst_iterator_free (it);

  bus = gst_element_get_bus (pipeline);
  gst_bus_add_signal_watch (bus);
  g_signal_connect (bus, "message", G_CALLBACK (on_message), NULL);
  gst_object_unref (bus);

  loop = g_main_loop_new (NULL, FALSE);

  if (gst_element_set_state (pipeline,
          GST_STATE_READY) == GST_STATE_CHANGE_FAILURE) {
    gst_object_unref (pipeline);
    g_main_loop_unref (loop);
    g_print ("Failed to set pipeline to ready\n");
    return -5;
  }

  service = g_socket_service_new ();
  g_socket_listener_add_inet_port (G_SOCKET_LISTENER (service), port, NULL,
      NULL);

  g_signal_connect (service, "incoming", G_CALLBACK (on_new_connection), NULL);

  g_socket_service_start (service);

  g_print ("Listening on http://127.0.0.1:%d/\n", port);

  g_main_loop_run (loop);

  g_socket_service_stop (service);
  g_object_unref (service);

  /* Lose our references to the endpoints */
  if (endpoints) {
    GList *l = endpoints;

    while (l) {
      EndPoint *endpoint = (EndPoint *) l->data;

      g_free (endpoint->content_type);
      g_free (endpoint->name);
      gst_object_unref (endpoint->element);
      g_free (endpoint);

      l = g_list_next (l);
    }

    g_list_free (endpoints);
    endpoints = NULL;
  }

  gst_element_set_state (pipeline, GST_STATE_NULL);
  gst_object_unref (pipeline);

  g_main_loop_unref (loop);

  return 0;
}
