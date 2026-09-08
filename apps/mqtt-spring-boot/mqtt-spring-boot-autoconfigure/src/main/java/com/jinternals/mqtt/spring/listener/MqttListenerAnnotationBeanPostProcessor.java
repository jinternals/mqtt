package com.jinternals.mqtt.spring.listener;

import com.jinternals.mqtt.spring.annotation.MqttListener;
import com.jinternals.mqtt.spring.core.MqttConnection;
import com.jinternals.mqtt.spring.core.MqttAcknowledgement;
import com.jinternals.mqtt.spring.core.MqttClientProperties;
import com.jinternals.mqtt.spring.core.MqttSubscription;
import com.jinternals.mqtt.spring.support.MqttCodec;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.env.Environment;
import org.springframework.util.ReflectionUtils;

/**
 * Turns {@link MqttListener} methods into {@link MqttSubscription}s.
 *
 * <h2>Why a BeanPostProcessor and not a BeanFactoryPostProcessor</h2>
 *
 * A {@code BeanFactoryPostProcessor} runs before any bean exists, so to find annotated methods it
 * has to scan the classpath itself — which means being told which packages to scan, and missing
 * anything defined by an {@code @Bean} method or registered programmatically, because those have no
 * scannable class-level stereotype.
 *
 * <p>A {@code BeanPostProcessor} is handed every bean instance the context actually creates. No
 * package configuration, no scanning, and it cannot miss a bean that exists. This is what
 * {@code @KafkaListener} and {@code @RabbitListener} both do.
 *
 * <p>Discovered subscriptions go into {@link MqttListenerRegistry} rather than straight to the
 * connection: a post-processor that injected {@code MqttConnection} would force it into existence
 * before the rest of the context was ready.
 *
 * <p>Dependencies are resolved lazily from the {@link BeanFactory} for the same reason — a
 * post-processor is created very early, and anything it injects through its constructor gets
 * dragged into early initialisation and skips post-processing itself.
 */
public class MqttListenerAnnotationBeanPostProcessor implements BeanPostProcessor, BeanFactoryAware {

    private static final Logger log =
            LoggerFactory.getLogger(MqttListenerAnnotationBeanPostProcessor.class);

    private final MqttListenerRegistry registry;
    private BeanFactory beanFactory;

    public MqttListenerAnnotationBeanPostProcessor(MqttListenerRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        // The target class, not the proxy: annotations on a @Transactional or @Async bean live on
        // the class behind the proxy, and selectMethods on the proxy would find nothing.
        Class<?> targetClass = AopUtils.getTargetClass(bean);

        Map<Method, MqttListener> annotated =
                MethodIntrospector.selectMethods(
                        targetClass,
                        (MethodIntrospector.MetadataLookup<MqttListener>)
                                method -> AnnotatedElementUtils.findMergedAnnotation(method, MqttListener.class));

        annotated.forEach((method, listener) -> register(bean, beanName, method, listener));
        return bean;
    }

    private void register(Object bean, String beanName, Method method, MqttListener listener) {
        validate(method);
        validateAckMode(beanName, method);

        String topic = environment().resolveRequiredPlaceholders(listener.topic());
        Method invocable = AopUtils.selectInvocableMethod(method, bean.getClass());
        ReflectionUtils.makeAccessible(invocable);

        Class<?>[] parameterTypes = method.getParameterTypes();
        Class<?> payloadType = parameterTypes[0];

        registry.register(
                new MqttSubscription(
                        topic,
                        listener.qos(),
                        (receivedTopic, payload, ack) ->
                                invoke(bean, invocable, parameterTypes, payloadType, receivedTopic, payload, ack)));

        log.info(
                "Registered @MqttListener {}#{} on '{}' qos={}",
                beanName,
                method.getName(),
                topic,
                listener.qos());
    }

    private void invoke(
            Object bean,
            Method method,
            Class<?>[] parameterTypes,
            Class<?> payloadType,
            String topic,
            byte[] raw,
            MqttAcknowledgement ack) {

        // Throws MqttPayloadConversionException, which the connection treats as unrecoverable:
        // dead-lettered if a dead-letter topic is configured, otherwise logged and acknowledged.
        // Either way it is never withheld for redelivery, because it cannot succeed on a retry.
        Object payload = convert(raw, payloadType);

        // Arguments after the payload are resolved by TYPE, not position, so (payload, ack) and
        // (payload, topic, ack) are both unambiguous.
        Object[] args = new Object[parameterTypes.length];
        args[0] = payload;
        for (int i = 1; i < parameterTypes.length; i++) {
            args[i] = parameterTypes[i] == MqttAcknowledgement.class ? ack : topic;
        }

        try {
            method.invoke(bean, args);
        } catch (InvocationTargetException e) {
            // Unwrap so the listener's own exception is what gets reported, not the reflection call.
            // Rethrowing matters: MqttConnection reads it as "not handled" and withholds the
            // acknowledgement, so the broker keeps the message for redelivery.
            throw new IllegalStateException(
                    "@MqttListener " + method.getName() + " failed for topic " + topic,
                    e.getTargetException());
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot invoke @MqttListener " + method.getName(), e);
        }
    }

    private Object convert(byte[] raw, Class<?> payloadType) {
        if (payloadType == byte[].class) {
            return raw;
        }
        if (payloadType == String.class) {
            return new String(raw, StandardCharsets.UTF_8);
        }
        return codec().decodeOrThrow(raw, payloadType);
    }

    private void validate(Method method) {
        Class<?>[] types = method.getParameterTypes();
        if (types.length < 1 || types.length > 3) {
            throw new IllegalStateException(
                    "@MqttListener method "
                            + method
                            + " must take (payload) plus optionally a String topic and/or an"
                            + " MqttAcknowledgement, in any order after the payload");
        }
        boolean seenTopic = false;
        boolean seenAck = false;
        for (int i = 1; i < types.length; i++) {
            if (types[i] == String.class) {
                if (seenTopic) {
                    throw new IllegalStateException(
                            "@MqttListener method " + method + " declares the topic parameter twice");
                }
                seenTopic = true;
            } else if (types[i] == MqttAcknowledgement.class) {
                if (seenAck) {
                    throw new IllegalStateException(
                            "@MqttListener method " + method + " declares the acknowledgement twice");
                }
                seenAck = true;
            } else {
                throw new IllegalStateException(
                        "@MqttListener method "
                                + method
                                + " has unsupported parameter "
                                + types[i].getName()
                                + "; after the payload only String (topic) and MqttAcknowledgement"
                                + " are resolved");
            }
        }
    }

    /**
     * Catches the two ways {@code mqtt.manual-acks} and a listener signature can disagree.
     *
     * <p>Both are startup failures rather than warnings, because both fail silently at runtime and
     * neither shows up in testing against a broker that never redelivers:
     *
     * <ul>
     *   <li><b>manual-acks on, no handle</b> — nothing ever acknowledges. The inflight window fills
     *       and delivery stops. No exception, no error log, just a feed that goes quiet.
     *   <li><b>manual-acks off, handle present</b> — the connection already acknowledges on return,
     *       so the parameter is not just redundant, it is a trap: acknowledge early, throw later,
     *       and the message is gone while the log claims it was withheld for redelivery.
     * </ul>
     */
    private void validateAckMode(String beanName, Method method) {
        MqttClientProperties properties =
                beanFactory.getBeanProvider(MqttClientProperties.class).getIfAvailable();
        if (properties == null) {
            // Wired by hand without the auto-configuration; the mode is not ours to infer.
            return;
        }
        boolean declaresAck = false;
        for (Class<?> type : method.getParameterTypes()) {
            declaresAck |= type == MqttAcknowledgement.class;
        }

        if (properties.isManualAcks() && !declaresAck) {
            throw new IllegalStateException(
                    "@MqttListener "
                            + beanName
                            + "#"
                            + method.getName()
                            + " takes no MqttAcknowledgement parameter, but "
                            + MqttClientProperties.PREFIX
                            + ".manual-acks is true. Nothing would ever acknowledge these messages"
                            + " and delivery would stall once the inflight window filled. Add an"
                            + " MqttAcknowledgement parameter, or set "
                            + MqttClientProperties.PREFIX
                            + ".manual-acks=false to let the connection acknowledge on return.");
        }
        if (!properties.isManualAcks() && declaresAck) {
            throw new IllegalStateException(
                    "@MqttListener "
                            + beanName
                            + "#"
                            + method.getName()
                            + " takes an MqttAcknowledgement parameter, but "
                            + MqttClientProperties.PREFIX
                            + ".manual-acks is false, so the connection already acknowledges once"
                            + " this method returns. Acknowledging early and then throwing would"
                            + " lose the message. Remove the parameter, or set "
                            + MqttClientProperties.PREFIX
                            + ".manual-acks=true to take ownership.");
        }
    }

    private Environment environment() {
        return beanFactory.getBean(Environment.class);
    }

    private MqttCodec codec() {
        return beanFactory.getBean(MqttCodec.class);
    }
}
