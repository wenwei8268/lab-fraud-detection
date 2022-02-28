package com.ververica.field.dynamicrules;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;


/**
 * 基于事件类型的风控
 * event ---> model ---------> strategy1 算法策略
 *                  ｜-------> strategy2 复杂事件规则的策略
 *                  ｜-------> strategy3 基于风控的请求数据，进行聚合，分析，处理得到进一步使用的特征；
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Event  implements TimestampAssignable<Long>{
  /**
   * 事件中心，有风控中心统一管理
   * 1：各业务方基于风控中心的统一分配ID
   * 2：
   */
  private Long eventId;
  /**
   * 事件发生的事件
   */
  public Long eventTime;

  /**
   * 业务主键
   */
  private Long businessId;


  private BigDecimal value;

  private Long ingestionTimestamp;


  /**
   * 每个事件带有事件JSON
   */
  public String paramsJson;

  @Override
  public void assignIngestionTimestamp(Long timestamp) {

  }
}
