package com.kl.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * description : 请求日志
 *
 * @author kunlunrepo
 * date :  2024-05-16 11:14
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AccessLogDO {

    private String title;

    private String url;

    private String method;

    private Integer httpCode;

    private String body;

    private Date createTime;

    private String userId;

    private String city;

}
