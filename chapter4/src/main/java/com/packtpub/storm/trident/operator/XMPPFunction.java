package com.packtpub.storm.trident.operator;

import com.packtpub.storm.trident.message.MessageMapper;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.jivesoftware.smack.AbstractXMPPConnection;
import org.jivesoftware.smack.ConnectionConfiguration;
import org.jivesoftware.smack.SASLAuthentication;
import org.jivesoftware.smack.chat2.Chat;
import org.jivesoftware.smack.chat2.ChatManager;
import org.jivesoftware.smack.packet.Presence;
import org.jivesoftware.smack.tcp.XMPPTCPConnection;
import org.jivesoftware.smack.tcp.XMPPTCPConnectionConfiguration;
import org.jxmpp.jid.EntityBareJid;
import org.jxmpp.jid.impl.JidCreate;
import org.jxmpp.stringprep.XmppStringprepException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by huangweidong on 2017/6/6
 */
public class XMPPFunction extends BaseFunction {
    private static final Logger LOG = LoggerFactory.getLogger(XMPPFunction.class);
    public static final String XMPP_TO = "admin";
    public static final String XMPP_USER = "storm";
    public static final String XMPP_PASSWORD = "root";
    public static final String XMPP_SERVER = "xmpp.storm.com";
    private static final int PORT = 5222;

    private AbstractXMPPConnection xmppConnection;
    private Chat chat;
    private EntityBareJid jid;
    private MessageMapper mapper;

    public XMPPFunction(MessageMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        LOG.debug("Prepare: {}", conf);
        super.prepare(conf, context);
        XMPPTCPConnectionConfiguration.Builder configBuilder = XMPPTCPConnectionConfiguration.builder();
        configBuilder.setUsernameAndPassword(XMPP_USER, XMPP_PASSWORD);
        configBuilder.setSecurityMode(ConnectionConfiguration.SecurityMode.disabled);
        configBuilder.setHost(XMPP_SERVER);
        configBuilder.setPort(PORT);

        try {
            configBuilder.setXmppDomain(XMPP_SERVER);
        } catch (XmppStringprepException e) {
            e.printStackTrace();
        }

        this.xmppConnection = new XMPPTCPConnection(configBuilder.build());

        try {
            xmppConnection.connect();
            SASLAuthentication.unBlacklistSASLMechanism("PLAIN");
            SASLAuthentication.blacklistSASLMechanism("DIGEST-MD5");
            xmppConnection.login();
            jid = JidCreate.entityBareFrom((String) conf.get(XMPP_TO));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        try {
            chat = ChatManager.getInstanceFor(xmppConnection).chatWith(jid);
            chat.send(mapper.toMessageBody(tuple));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
        xmppConnection.disconnect();
    }
}
