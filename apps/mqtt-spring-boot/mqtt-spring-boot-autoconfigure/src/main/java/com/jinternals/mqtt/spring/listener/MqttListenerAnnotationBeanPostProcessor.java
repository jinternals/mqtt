package com.jinternals.mqtt.spring.listener;

import com.jinternals.mqtt.spring.annotation.MqttListener;
import com.jinternals.mqtt.spring.core.MqttConnection;
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

        String topic = environment().resolveRequiredPlaceholders(listener.topic());
        Method invocable = AopUtils.selectInvocableMethod(method, bean.getClass());
        ReflectionUtils.makeAccessible(invocable);

        Class<?> payloadType = method.getParameterTypes()[0];
        boolean wantsTopic = method.getParameterCount() == 2;

        registry.register(
                new MqttSubscription(
                        topic,
                        listener.qos(),
                        (receivedTopic, payload) ->
                                invoke(bean, invocable, payloadType, wantsTopic, receivedTopic, payload)));

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
            Class<?> payloadType,
            boolean wantsTopic,
            String topic,
            byte[] raw) {

        Object payload = convert(raw, payloadType);
        if (payload == null) {
            // convert() already logged. Dropping one bad message must not break the subscription.
            return;
        }
        try {
            if (wantsTopic) {
                method.invoke(bean, payload, topic);
            } else {
                method.invoke(bean, payload);
            }
        } catch (InvocationTargetException e) {
            // Unwrap so the listener's own exception is what gets reported, not the reflection call.
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
        Object decoded = codec().decode(raw, payloadType);
        if (decoded == null) {
            log.warn("Undecodable {} payload, dropping", payloadType.getSimpleName());
        }
        return decoded;
    }

    private void validate(Method method) {
        int count = method.getParameterCount();
        if (count < 1 || count > 2) {
            throw new IllegalStateException(
                    "@MqttListener method "
                            + method
                            + " must take (payload) or (payload, String topic)");
        }
        if (count == 2 && method.getParameterTypes()[1] != String.class) {
            throw new IllegalStateException(
                    "@MqttListener method "
                            + method
                            + " second parameter must be String (the topic the message arrived on)");
        }
    }

    private Environment environment() {
        return beanFactory.getBean(Environment.class);
    }

    private MqttCodec codec() {
        return beanFactory.getBean(MqttCodec.class);
    }
}
