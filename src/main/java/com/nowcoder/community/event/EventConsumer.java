package com.nowcoder.community.event;

import com.alibaba.fastjson.JSONObject;
import com.nowcoder.community.entity.DiscussPost;
import com.nowcoder.community.entity.Event;
import com.nowcoder.community.entity.Message;
import com.nowcoder.community.service.DiscussPostService;
import com.nowcoder.community.service.ElasticsearchService;
import com.nowcoder.community.service.MessageService;
import com.nowcoder.community.util.CommunityConstant;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.nowcoder.community.util.CommunityConstant.*;

@Component
public class EventConsumer implements CommunityConstant{
    private static final Logger logger = LoggerFactory.getLogger(EventConsumer.class);
    @Autowired
    private MessageService messageService;
    @Autowired
    private DiscussPostService discussPostService;
    @Autowired
    private ElasticsearchService elasticsearchService;
    @Component
    @RocketMQMessageListener(topic = TOPIC_LIKE, consumerGroup = "${rocketmq.consumer.group}")
    class LikeConsumer implements RocketMQListener<JSONObject>{
        @Override
        public void onMessage(JSONObject record) {
            Consume(record);
        }
    }
    @Component
    @RocketMQMessageListener(topic = TOPIC_COMMENT, consumerGroup = "${rocketmq.consumer.group}")
    class CommentConsumer implements RocketMQListener<JSONObject>{
        @Override
        public void onMessage(JSONObject record) {
            Consume(record);
        }
    }
    @Component
    @RocketMQMessageListener(topic = TOPIC_FOLLOW, consumerGroup = "${rocketmq.consumer.group}")
    class FollowConsumer implements RocketMQListener<JSONObject>{
        @Override
        public void onMessage(JSONObject record) {
            Consume(record);
        }
    }
    public void Consume(JSONObject record){
        if (record == null) {
            logger.error("消息的内容为空!");
            return;
        }

        Event event = JSONObject.parseObject(record.toString(), Event.class);
        if (event == null) {
            logger.error("消息格式错误!");
            return;
        }
        System.out.println("收到消息"+record);
        // 发送站内通知
        System.out.println(event.getEntityId());
        System.out.println(event.getEntityUserId());
        Message message = new Message();
        message.setFromId(SYSTEM_USER_ID);
        message.setToId(event.getEntityUserId());
        message.setConversationId(event.getTopic());
        message.setCreateTime(new Date());

        Map<String, Object> content = new HashMap<>();
        content.put("userId", event.getUserId());
        content.put("entityType", event.getEntityType());
        content.put("entityId", event.getEntityId());

        if (!event.getData().isEmpty()) {
            for (Map.Entry<String, Object> entry : event.getData().entrySet()) {
                content.put(entry.getKey(), entry.getValue());
            }
        }

        message.setContent(JSONObject.toJSONString(content));
        messageService.addMessage(message);
    }


}
