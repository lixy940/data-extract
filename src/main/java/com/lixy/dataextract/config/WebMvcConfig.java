package com.lixy.dataextract.config;

import com.lixy.dataextract.intercepter.CorsRequestInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 *
 * Author：MR LIS，2019/10/24
 * Copyright(C) 2019 All rights reserved.
 */
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

    @Autowired
    private CorsRequestInterceptor corsRequestInterceptor;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // 多个拦截器组成一个拦截器链
        // addPathPatterns 用于添加拦截规则
        // excludePathPatterns 用户排除拦截
        registry.addInterceptor(corsRequestInterceptor).addPathPatterns("/**");


    }



}
