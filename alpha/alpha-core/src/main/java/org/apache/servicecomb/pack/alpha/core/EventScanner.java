/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.servicecomb.pack.alpha.core;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.servicecomb.pack.common.EventType.SagaEndedEvent;
import static org.apache.servicecomb.pack.common.EventType.TxAbortedEvent;
import static org.apache.servicecomb.pack.common.EventType.TxEndedEvent;
import static org.apache.servicecomb.pack.common.EventType.TxStartedEvent;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventScanner implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final byte[] EMPTY_PAYLOAD = new byte[0];

    private final ScheduledExecutorService scheduler;

    private final TxEventRepository eventRepository;

    private final CommandRepository commandRepository;

    private final TxTimeoutRepository timeoutRepository;

    private final OmegaCallback omegaCallback;

    private final int eventPollingInterval;

    private long nextEndedEventId;

    private long nextCompensatedEventId;

    private NodeStatus nodeStatus;

    public EventScanner(ScheduledExecutorService scheduler,
                        TxEventRepository eventRepository,
                        CommandRepository commandRepository,
                        TxTimeoutRepository timeoutRepository,
                        OmegaCallback omegaCallback,
                        int eventPollingInterval, NodeStatus nodeStatus) {
        this.scheduler = scheduler;
        this.eventRepository = eventRepository;
        this.commandRepository = commandRepository;
        this.timeoutRepository = timeoutRepository;
        this.omegaCallback = omegaCallback;
        this.eventPollingInterval = eventPollingInterval;
        this.nodeStatus = nodeStatus;
    }

    @Override
    public void run() {
        try {
            // Need to catch the exception to keep the event scanner running.
            pollEvents();
        } catch (Exception ex) {
            LOG.warn("Got the exception {} when pollEvents.", ex.getMessage(), ex);
        }
    }

    private void pollEvents() {
        //每 500ms 轮询一次
        scheduler.scheduleWithFixedDelay(
                () -> {
                    // only pull the events when working in the master mode
                    if (nodeStatus.isMaster()) {
                        //更新 TxTimeout 表中超时事件为已处理，标记为事务结束
                        updateTimeoutStatus();
                        //查找过期的事件, 并将过期事件转移到 TxTimeout 表中 （状态为 NEW）
                        findTimeoutEvents();
                        //取出超时的事件，生成 TxAbortedEvent 事件
                        abortTimeoutEvents();
                        //生成待补偿执行的 Command
                        saveUncompensatedEventsToCommands();
                        //获取 Command 补偿命令，触发补偿执行
                        compensate();
                        //更新 Command 状态为 DONE 已执行
                        updateCompensatedCommands();
                        //删除已经结束的事务
                        deleteDuplicateSagaEndedEvents();
                        //更新事务状态：即添加全局事务结束事件 SagaEndedEvent
                        updateTransactionStatus();
                    }
                },
                0,
                eventPollingInterval,   //默认500
                MILLISECONDS);
    }

    private void findTimeoutEvents() {
        //查找过期的事件
        eventRepository.findTimeoutEvents()
                .forEach(event -> {
                    LOG.info("Found timeout event {}", event);
                    //过期事件转移到 TxTimeout 表中 （状态为 NEW）
                    timeoutRepository.save(txTimeoutOf(event));
                });
    }

    private void updateTimeoutStatus() {
        timeoutRepository.markTimeoutAsDone();
    }

    private void saveUncompensatedEventsToCommands() {
        eventRepository.findFirstUncompensatedEventByIdGreaterThan(nextEndedEventId, TxEndedEvent.name())
                .forEach(event -> {
                    LOG.info("Found uncompensated event {}", event);
                    nextEndedEventId = event.id();
                    commandRepository.saveCompensationCommands(event.globalTxId());
                });
    }

    private void updateCompensatedCommands() {
        eventRepository.findFirstCompensatedEventByIdGreaterThan(nextCompensatedEventId)
                .ifPresent(event -> {
                    LOG.info("Found compensated event {}", event);
                    nextCompensatedEventId = event.id();
                    updateCompensationStatus(event);
                });
    }

    private void deleteDuplicateSagaEndedEvents() {
        try {
            eventRepository.deleteDuplicateEvents(SagaEndedEvent.name());
        } catch (Exception e) {
            LOG.warn("Failed to delete duplicate event", e);
        }
    }

    private void updateCompensationStatus(TxEvent event) {
        commandRepository.markCommandAsDone(event.globalTxId(), event.localTxId());
        LOG.info("Transaction with globalTxId {} and localTxId {} was compensated",
                event.globalTxId(),
                event.localTxId());

        markSagaEnded(event);
    }

    private void abortTimeoutEvents() {
        // 查询超时的事件(状态=NEW)，并重置状态为PENDING
        timeoutRepository.findFirstTimeout().forEach(timeout -> {
            LOG.info("Found timeout event {} to abort", timeout);

            //将PENDING状态的超时事件，重新生成一条 TxEvent 数据， 类型为 TxAbortedEvent 中断事件
            eventRepository.save(toTxAbortedEvent(timeout));

            //如果当前事件类型为 TxStartedEvent，则进行补偿执行
            if (timeout.type().equals(TxStartedEvent.name())) {
                eventRepository.findTxStartedEvent(timeout.globalTxId(), timeout.localTxId())
                        .ifPresent(omegaCallback::compensate);  //执行 PushBackOmegaCallback#compensate
            }
        });
    }

    private void updateTransactionStatus() {
        eventRepository.findFirstAbortedGlobalTransaction().ifPresent(this::markGlobalTxEndWithEvents);
    }

    private void markSagaEnded(TxEvent event) {
        if (commandRepository.findUncompletedCommands(event.globalTxId()).isEmpty()) {
            markGlobalTxEndWithEvent(event);
        }
    }

    private void markGlobalTxEndWithEvent(TxEvent event) {
        eventRepository.save(toSagaEndedEvent(event));
        LOG.info("Marked end of transaction with globalTxId {}", event.globalTxId());
    }

    private void markGlobalTxEndWithEvents(List<TxEvent> events) {
        events.forEach(this::markGlobalTxEndWithEvent);
    }

    private TxEvent toTxAbortedEvent(TxTimeout timeout) {
        return new TxEvent(
                timeout.serviceName(),
                timeout.instanceId(),
                timeout.globalTxId(),
                timeout.localTxId(),
                timeout.parentTxId(),
                TxAbortedEvent.name(),
                "",
                ("Transaction timeout").getBytes());
    }

    private TxEvent toSagaEndedEvent(TxEvent event) {
        return new TxEvent(
                event.serviceName(),
                event.instanceId(),
                event.globalTxId(),
                event.globalTxId(),
                null,
                SagaEndedEvent.name(),
                "",
                EMPTY_PAYLOAD);
    }

    private void compensate() {
        commandRepository.findFirstCommandToCompensate()
                .forEach(command -> {
                    LOG.info("Compensating transaction with globalTxId {} and localTxId {}",
                            command.globalTxId(),
                            command.localTxId());
                    //类型：PushBackOmegaCallback
                    omegaCallback.compensate(txStartedEventOf(command));
                });
    }

    private TxEvent txStartedEventOf(Command command) {
        return new TxEvent(
                command.serviceName(),
                command.instanceId(),
                command.globalTxId(),
                command.localTxId(),
                command.parentTxId(),
                TxStartedEvent.name(),
                command.compensationMethod(),
                command.payloads());
    }

    private TxTimeout txTimeoutOf(TxEvent event) {
        return new TxTimeout(
                event.id(),
                event.serviceName(),
                event.instanceId(),
                event.globalTxId(),
                event.localTxId(),
                event.parentTxId(),
                event.type(),
                event.expiryTime(),
                TaskStatus.NEW.name());
    }
}
