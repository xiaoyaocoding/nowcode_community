package com.nowcoder.community.event;

import com.alibaba.fastjson.JSONObject;
import com.nowcoder.community.entity.Event;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class EventProducer {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    // 处理事件
    public void fireEvent(Event event) {
        // 将事件发布到指定的主题
        rocketMQTemplate.convertAndSend(event.getTopic(), JSONObject.toJSONString(event));
    }

}
