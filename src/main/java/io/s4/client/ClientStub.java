/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 	        http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package io.s4.client;

import io.s4.client.util.ByteArrayIOChannel;
import io.s4.collector.EventWrapper;
import io.s4.listener.EventHandler;
import io.s4.message.Request;
import io.s4.message.Response;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

public abstract class ClientStub implements OutputStub, InputStub {

    protected static final Logger logger = Logger.getLogger("adapter");

    /**
     * Description of the protocol implemented by a concrete instance of this
     * stub.
     */
    public static class Info {
        public final String name;
        public final int versionMajor;
        public final int versionMinor;

        public Info(String name, int versionMajor, int versionMinor) {
            this.name = name;
            this.versionMajor = versionMajor;
            this.versionMinor = versionMinor;
        }
    }

    /**
     * Meta-information about the protocol that this stub uses to talk to
     * external clients.
     * 
     * This is sent to the client as a part of the handshake.
     */
    abstract public Info getProtocolInfo();

    /**
     * Stream names that are accepted by this stub to be forwarded to its
     * clients.
     */
    @Override
    public List<String> getAcceptedStreams() {
        return null;
    }

    private List<EventHandler> handlers = new ArrayList<EventHandler>();

    /**
     * A handler that can inject events produced by this stub into the S4
     * cluster.
     */
    @Override
    public void addHandler(EventHandler handler) {
        this.handlers.add(handler);
    }

    /**
     * Remove a handler.
     */
    @Override
    public boolean removeHandler(EventHandler handler) {
        return handlers.remove(handler);
    }

    /**
     * Convert an array of bytes into an event wrapper. This method is used to
     * translate data received from a client into events that may be injected
     * into the S4 cluster.
     * 
     * @param v
     *            array of bytes
     * @return EventWrapper constructed from the byte array.
     */
    abstract public EventWrapper eventWrapperFromBytes(byte[] v);

    /**
     * Convert an event wrapper into a byte array. Events received from the S4
     * cluster for dispatching to a client are translated into a byte array
     * using this method.
     * 
     * @param e
     *            an {@link EventWrapper}
     * @return a byte array
     */
    abstract public byte[] bytesFromEventWrapper(EventWrapper e);

    /**
     * Construct an I/O channel over which the stub can communicate with a
     * client. The channel allows arrys of bytes to be exchanged between the
     * stub and client.
     * 
     * @param socket
     *            TCP/IP socket
     * @return an IO Channel to send and recv byte arrays
     * @throws IOException
     *             if the underlying socket could not provide valid input and
     *             output streams.
     */
    public IOChannel createIOChannel(Socket socket) throws IOException {
        return new ByteArrayIOChannel(socket);
    }

    // send an event into the cluster via adapter.
    private void injectEvent(EventWrapper e) {
        for (EventHandler handler : handlers) {
            handler.processEvent(e);
        }
    }

    // private List<ClientConnection> clients = new
    // ArrayList<ClientConnection>();
    private HashMap<UUID, ClientConnection> clients = new HashMap<UUID, ClientConnection>();

    /**
     * Create a client connection and add it to list of clients.
     * 
     * @param socket
     *            client's I/O socket
     */
    private void addClient(ClientConnection c) {
        synchronized (clients) {
            logger.info("adding client " + c.uuid);
            clients.put(c.uuid, c);
        }
    }

    LinkedBlockingQueue<EventWrapper> queue = new LinkedBlockingQueue<EventWrapper>();

    @Override
    public int getQueueSize() {
        return queue.size();
    }

    @Override
    public void queueWork(EventWrapper e) {
        queue.offer(e);
    }

    ServerSocket serverSocket = null;

    public void setConnectionPort(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }

    private Thread acceptThread = null;
    private Thread senderThread = null;

    public void init() {
        // start accepting new clients and sending events to them
        (acceptThread = new Thread(connectionListener)).start();
        (senderThread = new Thread(sender)).start();
    }

    public void shutdown() {
        // stop accepting new clients
        if (acceptThread != null) {
            acceptThread.interrupt();
            acceptThread = null;
        }

        // stop sending events to them.
        if (senderThread != null) {
            senderThread.interrupt();
            senderThread = null;
        }

        // stop all connected clients.
        List<ClientConnection> clientCopy = new ArrayList<ClientConnection>(clients.values());
        for (ClientConnection c : clientCopy) {
            c.stop();
            c.close();
        }
    }

    private final Runnable connectionListener = new Runnable() {

        Handshake handshake = null;

        public void run() {
            if (handshake == null)
                handshake = new Handshake(ClientStub.this);

            try {
                while (serverSocket != null && serverSocket.isBound()
                        && !Thread.currentThread().isInterrupted()) {

                    Socket socket = serverSocket.accept();

                    ClientConnection connection = handshake.execute(socket);

                    if (connection != null) {
                        addClient(connection);
                        connection.start();
                    }

                }
            } catch (IOException e) {
                logger.info("exception in client connection listener", e);
            }
        }

    };

    public final Runnable sender = new Runnable() {
        ArrayList<ClientConnection> disconnect = new ArrayList<ClientConnection>();

        public void run() {

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    EventWrapper event = queue.take();
                    byte[] b = bytesFromEventWrapper(event);

                    // Responses need special handling.
                    if (event.getEvent() instanceof Response) {
                        dispatchResponse(event, b);
                        continue;
                    }

                    // TODO: include check to see if the event belongs to a
                    // particular client.

                    dispatchToAllClients(b);

                } catch (InterruptedException e) {
                    return;
                }
            }
        }

        private void dispatchToAllClients(byte[] b) {
            synchronized (clients) {
                for (ClientConnection c : clients.values()) {
                    if (c.good() && c.clientReadMode.takePublic()) {
                        try {
                            c.io.send(b);

                        } catch (IOException e) {
                            logger.error("error sending message to client "
                                    + c.uuid + ". disconnecting", e);

                            disconnect.add(c);
                        }
                    }
                }
            }

            if (disconnect.size() > 0) {
                for (ClientConnection d : disconnect)
                    d.close();

                disconnect.clear();
            }
        }

        private void dispatchResponse(EventWrapper event, byte[] b) {
            Response res = (Response) event.getEvent();
            Request.RInfo rinfo = res.getRInfo();

            if (rinfo instanceof Request.ClientRInfo) {
                UUID uuid = ((Request.ClientRInfo) rinfo).getRequesterUUID();

                ClientConnection c = clients.get(uuid);

                if (c != null && c.good() && c.clientReadMode.takePrivate()) {
                    try {
                        c.io.send(b);

                    } catch (IOException e) {
                        logger.error("error sending response to client "
                                + c.uuid + ". disconnecting", e);

                        c.close();
                    }

                } else {
                    logger.warn("no active client found for response: " + res);
                }
            }
        }
    };

    /**
     * Client mode.
     */
    public enum ClientReadMode {
        None(false, false), Private(true, false), All(true, true);

        private final boolean priv;
        private final boolean pub;

        ClientReadMode(boolean priv, boolean pub) {
            this.priv = priv;
            this.pub = pub;
        }

        public boolean takePublic() {
            return pub;
        }

        public boolean takePrivate() {
            return priv;
        }

        public static ClientReadMode fromString(String s) {
            if (s.equalsIgnoreCase("none"))
                return None;
            else if (s.equalsIgnoreCase("private"))
                return Private;
            else if (s.equalsIgnoreCase("all"))
                return All;
            else
                return null;
        }
    };

    /**
     * Client's write mode.
     */
    public enum ClientWriteMode {
        Enabled, Disabled;

        public static ClientWriteMode fromString(String s) {
            if (s.equalsIgnoreCase("enabled"))
                return Enabled;
            else if (s.equalsIgnoreCase("disabled"))
                return Disabled;
            else
                return null;
        }
    }

    /**
     * Connection to a client. A Stub has a collection of connections.
     */
    public class ClientConnection {
        /**
         * TCP/IP socket used to communicate with client.
         */
        private final Socket socket;

        public final IOChannel io;

        public final ClientReadMode clientReadMode;
        public final ClientWriteMode clientWriteMode;

        /**
         * GUID of client.
         */
        public final UUID uuid;

        public ClientConnection(Socket socket, UUID uuid,
                ClientReadMode clientReadMode, ClientWriteMode clientWriteMode)
                throws IOException {
            this.uuid = uuid;
            this.socket = socket;
            this.io = createIOChannel(socket);
            this.clientReadMode = clientReadMode;
            this.clientWriteMode = clientWriteMode;
        }

        public boolean good() {
            return socket.isConnected();
        }

        public void close() {
            synchronized (clients) {
                logger.info("closing connection to client " + uuid);
                clients.remove(this.uuid);
            }

            try {
                socket.close();
            } catch (IOException e) {
                logger.error("problem closing client connection to client "
                        + uuid, e);
            }
        }

        private Thread receiverThread = null;

        public void start() {
            if (clientWriteMode == ClientWriteMode.Enabled)
                (receiverThread = new Thread(receiver)).start();
        }

        public void stop() {
            if (receiverThread != null) {
                receiverThread.interrupt();
                receiverThread = null;
            }
        }

        public final Runnable receiver = new Runnable() {
            public void run() {
                try {
                    while (good()) {
                        byte[] b = io.recv();

                        // null, empty => goodbye
                        if (b == null || b.length == 0) {
                            logger.info("client session ended " + uuid);
                            break;
                        }

                        EventWrapper ew = eventWrapperFromBytes(b);
                        if (ew == null)
                            continue;

                        Object event = ew.getEvent();
                        if (event instanceof Request) {
                            decorateRequest((Request) event);
                            logger.info("Decorated client request: " + ew.toString());
                        }

                        injectEvent(ew);
                    }
                } catch (IOException e) {
                    logger.info("error while reading from client " + uuid, e);

                } finally {
                    close();
                }
            }

            private void decorateRequest(Request r) {
                // add UUID of client into request.
                Request.RInfo info = r.getRInfo();

                if (info != null && info instanceof Request.ClientRInfo)
                    ((Request.ClientRInfo) info).setRequesterUUID(ClientConnection.this.uuid);
            }
        };

    }
}
