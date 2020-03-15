package com.spark.service;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;

import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
@ServerEndpoint("/websocket")
public class WeblogSocket {

    WeblogService  weblogService = new WeblogService();
    @OnMessage
    public void onMessage(String message, Session session)
            throws IOException, InterruptedException {
        while(true){
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("titleName", weblogService.queryWeblogs().get("titleName"));
            map.put("titleCount",weblogService.queryWeblogs().get("titleCount"));
            map.put("titleSum", weblogService.titleCount());

            session.getBasicRemote().
                    sendText(JSON.toJSONString(map));
            Thread.sleep(5000);
            map.clear();
        }
    }
    @OnOpen
    public void onOpen () {
        System.out.println("Client connected");
    }
    @OnClose
    public void onClose () {
        System.out.println("Connection closed");
    }
}
