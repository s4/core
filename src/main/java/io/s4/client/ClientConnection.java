package io.s4.client;

import io.s4.collector.EventWrapper;
import io.s4.message.Request;

import java.io.IOException;
import java.net.Socket;
import java.util.UUID;

/**
 * Connection to a client. A Stub has a collection of connections.
 */
public class ClientConnection {
    /**
     * 
     */
    private final ClientStub clientStub;

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

    public ClientConnection(ClientStub clientStub, Socket socket, UUID uuid,
            ClientReadMode clientReadMode, ClientWriteMode clientWriteMode)
            throws IOException {
        this.clientStub = clientStub;
        this.uuid = uuid;
        this.socket = socket;
        this.io = this.clientStub.createIOChannel(socket);
        this.clientReadMode = clientReadMode;
        this.clientWriteMode = clientWriteMode;
    }

    public boolean good() {
        return socket.isConnected();
    }

    public void close() {
        synchronized (this.clientStub.clients) {
            ClientStub.logger.info("closing connection to client " + uuid);
            this.clientStub.clients.remove(this.uuid);
        }

        try {
            socket.close();
        } catch (IOException e) {
            ClientStub.logger.error("problem closing client connection to client "
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
                        ClientStub.logger.info("client session ended " + uuid);
                        break;
                    }

                    EventWrapper ew = ClientConnection.this.clientStub.eventWrapperFromBytes(b);
                    if (ew == null)
                        continue;

                    Object event = ew.getEvent();
                    if (event instanceof Request) {
                        decorateRequest((Request) event);
                        ClientStub.logger.info("Decorated client request: " + ew.toString());
                    }

                    ClientConnection.this.clientStub.injectEvent(ew);
                }
            } catch (IOException e) {
                ClientStub.logger.info("error while reading from client " + uuid, e);

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