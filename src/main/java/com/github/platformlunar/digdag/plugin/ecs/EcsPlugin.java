package com.github.platformlunar.digdag.plugin.ecs;

import io.digdag.core.Environment;
import io.digdag.client.config.Config;

import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorProvider;
import io.digdag.spi.Plugin;
import io.digdag.spi.TemplateEngine;

import java.util.Arrays;
import java.util.Map;
import java.util.List;

import javax.inject.Inject;

public class EcsPlugin implements Plugin {
    @Override
    public <T> Class<? extends T> getServiceProvider(Class<T> type) {
        if (type == OperatorProvider.class) {
            return EcsOperatorProvider.class.asSubclass(type);
        } else {
            return null;
        }
    }

    public static class EcsOperatorProvider implements OperatorProvider {
        @Inject
        protected TemplateEngine templateEngine;

        @Inject
        Config systemConfig;

        @Inject
        @Environment
        protected Map<String, String> environment;

        @Override
        public List<OperatorFactory> get() {
            return Arrays.asList(new EcsRegisterOperatorFactory(templateEngine, environment),
                                 new EcsRunOperatorFactory(templateEngine, environment));
        }
    }
}
