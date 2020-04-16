package com.michael.enums

object ChannelEnumeration extends Enumeration {

  /** IOS */
  val IOS: ChannelEnumeration.Value = Value(1)
  /** Android */
  val ANDROID: ChannelEnumeration.Value = Value(2)
  /** 微信小程序 */
  val WECHAT_APP: ChannelEnumeration.Value = Value(3)
  /** 微信公众号 */
  val WECHAT_PUBLIC: ChannelEnumeration.Value = Value(4)
}
