package com.michael.vo

import com.michael.caseclass.{MemberChannel, MemberHeat, MemberMpSub, MemberSex}

/**
 * @author chudichen
 * @since 2020/4/16
 */
case class MemberVO(memberChannels: java.util.List[MemberChannel],
                    memberSexes: java.util.List[MemberSex],
                    memberMpSubs: MemberMpSub,
                    memberHeat: MemberHeat
)
