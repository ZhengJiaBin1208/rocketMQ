package com.zjb.rocketmq.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @ClassName FilterTypeEnum
 * @Description 过滤类型
 * @Author zhengjiabin
 * @Date 2023/7/5 15:55
 * @Version 1.0
 **/
@Getter
@AllArgsConstructor
public enum FilterTypeEnum {

    SQL_TYPE("sql");

    String type;

}
