package com.github.xhanshawn.utils

object CURColumnsDefinitions {
  /**
    * Contants for common used CUR columns.
    */
  val RICoreColumns = List(
    "reservation/ReservationARN",
    "lineItem/UsageType",
    "reservation/ModificationStatus",
    "reservation/NumberOfReservations",
    "reservation/StartTime",
    "reservation/EndTime"
  )
  val RIUsageColumns = List(
    "reservation/TotalReservedUnits",
    "reservation/UnitsPerReservation",
    "reservation/NumberOfReservations",
    "reservation/NormalizedUnitsPerReservation"
  )
  val RICostColumns = List(
    "reservation/UpfrontValue",
    "reservation/EffectiveCost",
    "lineItem/UnblendedRate"
  )
  val UnusedRIColumns = List(
    "reservation/UnusedQuantity",
    "reservation/UnusedRecurringFee",
    "reservation/UnusedNormalizedUnitQuantity",
    "reservation/UnusedAmortizedUpfrontFeeForBillingPeriod"
  )
  val AmortizationColumns = List(
    "reservation/AmortizedUpfrontCostForUsage",
    "reservation/AmortizedUpfrontFeeForBillingPeriod",
    "reservation/UnusedAmortizedUpfrontFeeForBillingPeriod",
    "reservation/UpfrontValue"
  )
  val RIColumns = List(
    RICoreColumns,
    RICostColumns,
    UnusedRIColumns,
    AmortizationColumns).flatten.distinct
  val RIModelColumns = List(
    RICoreColumns,
    List(
      "reservation/UpfrontValue",
      "lineItem/UsageAccountId")
  ).flatten.distinct
}
