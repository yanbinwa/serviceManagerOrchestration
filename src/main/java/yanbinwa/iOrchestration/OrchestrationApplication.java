package yanbinwa.iOrchestration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.embedded.ConfigurableEmbeddedServletContainer;
import org.springframework.boot.context.embedded.EmbeddedServletContainerCustomizer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableAutoConfiguration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "tomcatPort")
public class OrchestrationApplication implements EmbeddedServletContainerCustomizer
{
    @Value("${tomcatPort:}")
    int tomcatPort;
    
    public static void main(String[] args)
    {
        SpringApplication.run(OrchestrationApplication.class, args);
    }

    @Override
    public void customize(ConfigurableEmbeddedServletContainer container)
    {
        container.setPort(tomcatPort);
    }
}
