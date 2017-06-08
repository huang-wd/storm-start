package com.xmpp;

import org.jivesoftware.smack.ConnectionConfiguration;
import org.jivesoftware.smack.SASLAuthentication;
import org.jivesoftware.smack.SmackException;
import org.jivesoftware.smack.XMPPException;
import org.jivesoftware.smack.chat2.Chat;
import org.jivesoftware.smack.chat2.ChatManager;
import org.jivesoftware.smack.tcp.XMPPTCPConnection;
import org.jivesoftware.smack.tcp.XMPPTCPConnectionConfiguration;
import org.jxmpp.jid.EntityBareJid;
import org.jxmpp.jid.impl.JidCreate;

import java.io.IOException;

/**
 * Created by huangweidong on 2017/6/8
 */
public class XMPPTest {
    public static void main(String[] args) throws IOException, SmackException, InterruptedException, XMPPException {
        XMPPTCPConnectionConfiguration.Builder config = XMPPTCPConnectionConfiguration.builder();
        config.setSecurityMode(ConnectionConfiguration.SecurityMode.disabled);
        config.setUsernameAndPassword("storm", "root");
        config.setHost("xmpp.storm.com");
        config.setXmppDomain("xmpp.storm.com");
        config.setPort(5222);
        config.setDebuggerEnabled(true);

        XMPPTCPConnection mConnection = new XMPPTCPConnection(config.build());
        mConnection.connect();

        SASLAuthentication.unBlacklistSASLMechanism("PLAIN");
        SASLAuthentication.blacklistSASLMechanism("DIGEST-MD5");
        mConnection.login();

        EntityBareJid jid = JidCreate.entityBareFrom("admin@xmpp.storm.com");
        // Start a new conversation with John Doe and send him a message.
        Chat chat = ChatManager.getInstanceFor(mConnection).chatWith(jid);

        chat.send("hello xmpp");
        // Disconnect from the server
        mConnection.disconnect();
    }
}
