package com.michael.tag.entity;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author chudichen
 * @since 2020/4/22
 */
@Data
public class MemberTag implements Serializable {
    // i_member.t_member
    private String memberId;
    private String phone;
    private String sex;
    private String channel;
    private String subOpenId;
    private String address;
    private String regTime;
    // i_member.t_member

    // i_order
    private Long orderCount;
    // max(create_time) i_order.t_order
    private String orderTime;
    private Double orderMoney;
    private List<String> favGoods;
    // i_order

    // i_marketing
    private String freeCouponTime;
    private List<String> couponTimes;
    private Double chargeMoney;
    // i_marketing

    private Integer overTime;
    private Integer feedBack;
}
