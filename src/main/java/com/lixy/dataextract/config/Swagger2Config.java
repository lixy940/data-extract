package com.lixy.dataextract.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;

import java.sql.Timestamp;

@Configuration
public class Swagger2Config {

    @Bean
    Docket docket() {
        return new Docket(DocumentationType.SWAGGER_2).apiInfo(apiInfo()).select()
                .apis(RequestHandlerSelectors.basePackage("com.lixy.dataextract.controller"))
                .paths(PathSelectors.any())
                .build()
//                .globalOperationParameters(setHeaderToken())
                .directModelSubstitute(Timestamp.class, Long.class);
    }

    @Bean
    ApiInfo apiInfo() {
        return new ApiInfoBuilder().title("data-extract  api docs")
                .version("1.0.0")
                .termsOfServiceUrl("www.baidu.com")
                .license("")
                .licenseUrl("www.baidu.com")
                .build();
    }

  /*  private List<Parameter> setHeaderToken() {
        ParameterBuilder tokenPar = new ParameterBuilder();
        List<Parameter> pars = new ArrayList<>();
        tokenPar.name(StaticParameterUtils.HTTP_HEADER_TOKEN).description("X-Auth-Token").modelRef(new ModelRef("string")).parameterType("header").required(false).defaultValue(StaticParameterUtils.RPC_TOKEN).build();
        pars.add(tokenPar.build());
        return pars;
    }*/

}
