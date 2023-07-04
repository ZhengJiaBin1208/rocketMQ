package com.zjb.rocketmq.batch;

import org.apache.rocketmq.common.message.Message;

import java.util.List;
import java.util.Iterator;
import java.util.Map;

/**
 * @ClassName ListSplitter
 * @Description 把大的消息分裂成若干个小的消息（1M左右）
 * @Author zhengjiabin
 * @Date 2023/7/4 10:42
 * @Version 1.0
 **/
class ListSplitter implements Iterator<List<Message>> {
    // 1M
    private int sizeLimit = 1000 * 1000;
    // 是要被分割的原始List<Message>对象。
    private final List<Message> messages;
    // 是当前子List<Message>对象起始位置的下标。
    private  int currIndex;
    public ListSplitter(List<Message> messages) {
        this.messages = messages;
    }

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override
    public boolean hasNext() {
        return currIndex < messages.size();
    }

    @Override
    public List<Message> next(){
        int nextIndex = currIndex;
        int totalSize = 0;
        for (;  nextIndex < messages.size();nextIndex ++){
            Message message = messages.get(nextIndex);
            int tmpSize = message.getTopic().length() + message.getBody().length;
            Map<String, String> properties = message.getProperties();
            // 把属性也要算进去
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                tmpSize += entry.getKey().length() + entry.getValue().length();
            }
            // 增加日志的开销20字节
            tmpSize += 20;
            if (tmpSize > sizeLimit) {
                // 单个消息超过了最大的限制(1M),否则阻塞进程
                if (nextIndex - currIndex == 0) {
                    // 加入下一个子列表没有元素,则添加这个子列表然后退出循环,否则退出循环
                    nextIndex++;
                }
                break;
            }
            if (tmpSize + totalSize > sizeLimit) {
                break;
            }else {
                totalSize += tmpSize;
            }
        }
        List<Message> subList = messages.subList(currIndex, nextIndex);
        currIndex = nextIndex;
        return subList;
    }
}
