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

import io.s4.client.ClientStub.ClientConnection;
import io.s4.client.ClientStub.Info;
import io.s4.client.util.ByteArrayIOChannel;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;

public class Handshake {
    private final ClientStub clientStub;

    protected static final Logger logger = Logger.getLogger("adapter");

    protected final Gson gson = new Gson();

    public Handshake(ClientStub clientStub) {
        this.clientStub = clientStub;
    }

    // execute handshake with client.
    // 1. discovery: issue uuid;
    // 2. main connect: create connection
    public ClientConnection execute(Socket s) {
        byte[] v;

        try {
            ClientConnection conn = null;

            ByteArrayIOChannel io = new ByteArrayIOChannel(s);

            v = io.recv();

            if (v == null || v.length == 0) {
                // no information => client initialization
                clientInit(io);

            } else {
                // some data => client connect
                conn = clientConnect(v, io, s);
            }

            if (conn == null)
                s.close();

            return conn;

        } catch (IOException e) {
            logger.error("exception during handshake", e);
            try {
                s.close();
                return null;

            } catch (IOException ee) {
                throw new RuntimeException("failed to close socket after failed handshake",
                                           ee);
            }
        }
    }

    private ClientConnection clientConnect(byte[] v, ByteArrayIOChannel io,
                                           Socket sock) throws IOException {

        List<String> reason = new ArrayList<String>(1);
        ClientConnection conn = clientConnectCreate(v, io, sock, reason);

        String message = null;
        try {
            JSONObject resp = new JSONObject();

            resp.put("status", (conn != null ? "ok" : "failed"));

            if (conn == null && !reason.isEmpty()) {
                resp.put("reason", reason.get(0));
            }

            message = resp.toString();

        } catch (JSONException e) {
            logger.error("error creating response during connect.", e);
            return null;
        }
        
        io.send(message.getBytes());
        
        return conn;
    }

    private ClientConnection clientConnectCreate(byte[] v,
                                                 ByteArrayIOChannel io,
                                                 Socket sock,
                                                 List<String> reason)
            throws IOException {

        try {
            JSONObject cInfo = new JSONObject(new String(v));

            String s = cInfo.optString("uuid", "");
            if (s.isEmpty()) {
                logger.error("missing client identifier during handshake.");
                reason.add("missing UUID");
                return null;
            }

            UUID u = UUID.fromString(s);

            logger.info("connecting to client " + u);

            s = cInfo.optString("readMode", "All");
            ClientStub.ClientReadMode rmode = ClientStub.ClientReadMode.fromString(s);
            if (rmode == null) {
                logger.error(u + ": unknown readMode " + s);
                reason.add("unknown readMode " + s);
                return null;
            }

            s = cInfo.optString("writeMode", "Enabled");
            ClientStub.ClientWriteMode wmode = ClientStub.ClientWriteMode.fromString(s);
            if (wmode == null) {
                logger.error(u + ": unknown writeMode " + s);
                reason.add("unknown writeMode " + s);
                return null;
            }

            logger.info(u + " read=" + rmode + " write=" + wmode);

            return clientStub.new ClientConnection(sock, u, rmode, wmode);

        } catch (JSONException e) {
            logger.error("malformed JSON from client during handshake", e);
            reason.add("malformed JSON");

        } catch (NumberFormatException e) {
            logger.error("received malformed UUID", e);
            reason.add("malformed UUID");

        } catch (IllegalArgumentException e) {
            logger.error("received malformed UUID", e);
            reason.add("malformed UUID");
        }

        return null;
    }

    private void clientInit(ByteArrayIOChannel io) throws IOException {
        String uuid = UUID.randomUUID().toString();
        Info proto = clientStub.getProtocolInfo();

        HashMap<String, Object> info = new HashMap<String, Object>();
        info.put("uuid", uuid);
        info.put("protocol", proto);

        String response = gson.toJson(info) + "\n";

        io.send(response.getBytes());
    }

}