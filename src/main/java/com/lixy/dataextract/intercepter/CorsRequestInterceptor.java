package com.lixy.dataextract.intercepter;

import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * 前端跨域问题处理
 * @author silent
 */
@Component
public class CorsRequestInterceptor implements HandlerInterceptor {

    @Override
    public void afterCompletion(HttpServletRequest arg0,
                                HttpServletResponse arg1, Object arg2, Exception arg3) {
    }

    @Override
    public void postHandle(HttpServletRequest arg0, HttpServletResponse response,
                           Object arg2, ModelAndView arg3) {
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response,
                             Object arg2) {
        /**
         * 勉强解决前端跨域问题，最最有效的方式还是部署nginx
         */
        if (request.getMethod().toLowerCase().equals("options")) {
            response.setStatus(200);
        }
        String origin = request.getHeader("Origin");
        response.setHeader("Access-Control-Allow-Credentials", "true");
        response.setHeader("Access-Control-Allow-Origin", origin);
        response.setHeader("Access-Control-Allow-Methods", "POST,GET,DELETE,PUT,OPTIONS");
        response.setHeader("Access-Control-Allow-Headers", "X-Requested-With,Content-Type,Accept,Token,token,Remote-User,content-type");
        return true;
    }
}
