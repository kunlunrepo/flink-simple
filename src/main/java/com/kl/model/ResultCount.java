package com.kl.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * description : 结果统计
 *
 * @author kunlunrepo
 * date :  2024-05-16 11:16
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ResultCount {

    private String url;

    private Integer code;

    private Long count;

    private String startTime;

    private String endTime;

    private String type;

}
