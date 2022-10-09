/// Adds the schema of the common type imports
pub(crate) fn add_common_files(imports: &Vec<String>, files: &mut Vec<String>) {
    for i in imports {
        let common_type = if let Some(ct) = is_common_import(i) {
            ct
        } else {
            continue;
        };
        for cs in get_schemas(common_type) {
            files.push(String::from(get_schema(cs)))
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum CommonType {
    CalendarPeriod,
    Color,
    Date,
    DateTime,
    DayOfWeek,
    Decimal,
    Expr,
    Fraction,
    Interval,
    LatLng,
    LocalizedText,
    Money,
    Month,
    PhoneNumber,
    PostalAddress,
    Quaternion,
    TimeOfDay,
    Type,
}

#[derive(Clone, Debug, PartialEq)]
enum CommonSchema {
    CalendarPeriod,
    Color,
    Date,
    DateTime,
    DayOfWeek,
    Decimal,
    Duration,
    Expr,
    Fraction,
    Interval,
    LatLng,
    LocalizedText,
    Money,
    Month,
    PhoneNumber,
    PostalAddress,
    Quaternion,
    TimeOfDay,
    Timestamp,
    Type,
    Wrappers,
}

fn is_common_import(import: &str) -> Option<CommonType> {
    match import {
        "google/type/calendar_period.proto" => Some(CommonType::CalendarPeriod),
        "google/type/color.proto" => Some(CommonType::Color),
        "google/type/date.proto" => Some(CommonType::Date),
        "google/type/datetime.proto" => Some(CommonType::DateTime),
        "google/type/dayofweek.proto" => Some(CommonType::DayOfWeek),
        "google/type/decimal.proto" => Some(CommonType::Decimal),
        "google/type/expr.proto" => Some(CommonType::Expr),
        "google/type/fraction.proto" => Some(CommonType::Fraction),
        "google/type/interval.proto" => Some(CommonType::Interval),
        "google/type/latlng.proto" => Some(CommonType::LatLng),
        "google/type/localized_text.proto" => Some(CommonType::LocalizedText),
        "google/type/money.proto" => Some(CommonType::Money),
        "google/type/month.proto" => Some(CommonType::Month),
        "google/type/phone_number.proto" => Some(CommonType::PhoneNumber),
        "google/type/postal_address.proto" => Some(CommonType::PostalAddress),
        "google/type/quaternion.proto" => Some(CommonType::Quaternion),
        "google/type/timeofday.proto" => Some(CommonType::TimeOfDay),
        "google/type/type.proto" => Some(CommonType::Type),
        _ => None,
    }
}

fn get_schemas(common_type: CommonType) -> &'static [CommonSchema] {
    match common_type {
        CommonType::CalendarPeriod => &[CommonSchema::CalendarPeriod],
        CommonType::Color => &[CommonSchema::Wrappers, CommonSchema::Color],
        CommonType::Date => &[CommonSchema::Date],
        CommonType::DateTime => &[CommonSchema::Duration, CommonSchema::DateTime],
        CommonType::DayOfWeek => &[CommonSchema::DayOfWeek],
        CommonType::Decimal => &[CommonSchema::Decimal],
        CommonType::Expr => &[CommonSchema::Expr],
        CommonType::Fraction => &[CommonSchema::Fraction],
        CommonType::Interval => &[CommonSchema::Timestamp, CommonSchema::Interval],
        CommonType::LatLng => &[CommonSchema::LatLng],
        CommonType::LocalizedText => &[CommonSchema::LocalizedText],
        CommonType::Money => &[CommonSchema::Money],
        CommonType::Month => &[CommonSchema::Month],
        CommonType::PhoneNumber => &[CommonSchema::PhoneNumber],
        CommonType::PostalAddress => &[CommonSchema::PostalAddress],
        CommonType::Quaternion => &[CommonSchema::Quaternion],
        CommonType::TimeOfDay => &[CommonSchema::TimeOfDay],
        CommonType::Type => &[CommonSchema::Type],
    }
}

fn get_schema(common_schema: &CommonSchema) -> &'static str {
    match common_schema {
        CommonSchema::CalendarPeriod => {
            r#"// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";

package google.type;

option go_package = "google.golang.org/genproto/googleapis/type/calendarperiod;calendarperiod";
option java_multiple_files = true;
option java_outer_classname = "CalendarPeriodProto";
option java_package = "com.google.type";
option objc_class_prefix = "GTP";

// A `CalendarPeriod` represents the abstract concept of a time period that has
// a canonical start. Grammatically, "the start of the current
// `CalendarPeriod`." All calendar times begin at midnight UTC.
enum CalendarPeriod {
  // Undefined period, raises an error.
  CALENDAR_PERIOD_UNSPECIFIED = 0;

  // A day.
  DAY = 1;

  // A week. Weeks begin on Monday, following
  // [ISO 8601](https://en.wikipedia.org/wiki/ISO_week_date).
  WEEK = 2;

  // A fortnight. The first calendar fortnight of the year begins at the start
  // of week 1 according to
  // [ISO 8601](https://en.wikipedia.org/wiki/ISO_week_date).
  FORTNIGHT = 3;

  // A month.
  MONTH = 4;

  // A quarter. Quarters start on dates 1-Jan, 1-Apr, 1-Jul, and 1-Oct of each
  // year.
  QUARTER = 5;

  // A half-year. Half-years start on dates 1-Jan and 1-Jul.
  HALF = 6;

  // A year.
  YEAR = 7;
}"#
        }
        CommonSchema::Color => {
            r#"// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";

package google.type;

import "google/protobuf/wrappers.proto";

option cc_enable_arenas = true;
option go_package = "google.golang.org/genproto/googleapis/type/color;color";
option java_multiple_files = true;
option java_outer_classname = "ColorProto";
option java_package = "com.google.type";
option objc_class_prefix = "GTP";

// Represents a color in the RGBA color space. This representation is designed
// for simplicity of conversion to/from color representations in various
// languages over compactness. For example, the fields of this representation
// can be trivially provided to the constructor of `java.awt.Color` in Java; it
// can also be trivially provided to UIColor's `+colorWithRed:green:blue:alpha`
// method in iOS; and, with just a little work, it can be easily formatted into
// a CSS `rgba()` string in JavaScript.
//
// This reference page doesn't carry information about the absolute color
// space
// that should be used to interpret the RGB value (e.g. sRGB, Adobe RGB,
// DCI-P3, BT.2020, etc.). By default, applications should assume the sRGB color
// space.
//
// When color equality needs to be decided, implementations, unless
// documented otherwise, treat two colors as equal if all their red,
// green, blue, and alpha values each differ by at most 1e-5.
//
// Example (Java):
//
//      import google/type/Color;
//
//      // ...
//      public static java.awt.Color fromProto(Color protocolor) {
//        float alpha = protocolor.hasAlpha()
//            ? protocolor.getAlpha().getValue()
//            : 1.0;
//
//        return new java.awt.Color(
//            protocolor.getRed(),
//            protocolor.getGreen(),
//            protocolor.getBlue(),
//            alpha);
//      }
//
//      public static Color toProto(java.awt.Color color) {
//        float red = (float) color.getRed();
//        float green = (float) color.getGreen();
//        float blue = (float) color.getBlue();
//        float denominator = 255.0;
//        Color.Builder resultBuilder =
//            Color
//                .newBuilder()
//                .setRed(red / denominator)
//                .setGreen(green / denominator)
//                .setBlue(blue / denominator);
//        int alpha = color.getAlpha();
//        if (alpha != 255) {
//          result.setAlpha(
//              FloatValue
//                  .newBuilder()
//                  .setValue(((float) alpha) / denominator)
//                  .build());
//        }
//        return resultBuilder.build();
//      }
//      // ...
//
// Example (iOS / Obj-C):
//
//      // ...
//      static UIColor* fromProto(Color* protocolor) {
//         float red = [protocolor red];
//         float green = [protocolor green];
//         float blue = [protocolor blue];
//         FloatValue* alpha_wrapper = [protocolor alpha];
//         float alpha = 1.0;
//         if (alpha_wrapper != nil) {
//           alpha = [alpha_wrapper value];
//         }
//         return [UIColor colorWithRed:red green:green blue:blue alpha:alpha];
//      }
//
//      static Color* toProto(UIColor* color) {
//          CGFloat red, green, blue, alpha;
//          if (![color getRed:&red green:&green blue:&blue alpha:&alpha]) {
//            return nil;
//          }
//          Color* result = [[Color alloc] init];
//          [result setRed:red];
//          [result setGreen:green];
//          [result setBlue:blue];
//          if (alpha <= 0.9999) {
//            [result setAlpha:floatWrapperWithValue(alpha)];
//          }
//          [result autorelease];
//          return result;
//     }
//     // ...
//
//  Example (JavaScript):
//
//     // ...
//
//     var protoToCssColor = function(rgb_color) {
//        var redFrac = rgb_color.red || 0.0;
//        var greenFrac = rgb_color.green || 0.0;
//        var blueFrac = rgb_color.blue || 0.0;
//        var red = Math.floor(redFrac * 255);
//        var green = Math.floor(greenFrac * 255);
//        var blue = Math.floor(blueFrac * 255);
//
//        if (!('alpha' in rgb_color)) {
//           return rgbToCssColor(red, green, blue);
//        }
//
//        var alphaFrac = rgb_color.alpha.value || 0.0;
//        var rgbParams = [red, green, blue].join(',');
//        return ['rgba(', rgbParams, ',', alphaFrac, ')'].join('');
//     };
//
//     var rgbToCssColor = function(red, green, blue) {
//       var rgbNumber = new Number((red << 16) | (green << 8) | blue);
//       var hexString = rgbNumber.toString(16);
//       var missingZeros = 6 - hexString.length;
//       var resultBuilder = ['#'];
//       for (var i = 0; i < missingZeros; i++) {
//          resultBuilder.push('0');
//       }
//       resultBuilder.push(hexString);
//       return resultBuilder.join('');
//     };
//
//     // ...
message Color {
  // The amount of red in the color as a value in the interval [0, 1].
  float red = 1;

  // The amount of green in the color as a value in the interval [0, 1].
  float green = 2;

  // The amount of blue in the color as a value in the interval [0, 1].
  float blue = 3;

  // The fraction of this color that should be applied to the pixel. That is,
  // the final pixel color is defined by the equation:
  //
  //   `pixel color = alpha * (this color) + (1.0 - alpha) * (background color)`
  //
  // This means that a value of 1.0 corresponds to a solid color, whereas
  // a value of 0.0 corresponds to a completely transparent color. This
  // uses a wrapper message rather than a simple float scalar so that it is
  // possible to distinguish between a default value and the value being unset.
  // If omitted, this color object is rendered as a solid color
  // (as if the alpha value had been explicitly given a value of 1.0).
  google.protobuf.FloatValue alpha = 4;
}"#
        }
        CommonSchema::Date => {
            r#"// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";

package google.type;

option cc_enable_arenas = true;
option go_package = "google.golang.org/genproto/googleapis/type/date;date";
option java_multiple_files = true;
option java_outer_classname = "DateProto";
option java_package = "com.google.type";
option objc_class_prefix = "GTP";

// Represents a whole or partial calendar date, such as a birthday. The time of
// day and time zone are either specified elsewhere or are insignificant. The
// date is relative to the Gregorian Calendar. This can represent one of the
// following:
//
// * A full date, with non-zero year, month, and day values
// * A month and day value, with a zero year, such as an anniversary
// * A year on its own, with zero month and day values
// * A year and month value, with a zero day, such as a credit card expiration
// date
//
// Related types are [google.type.TimeOfDay][google.type.TimeOfDay] and
// `google.protobuf.Timestamp`.
message Date {
  // Year of the date. Must be from 1 to 9999, or 0 to specify a date without
  // a year.
  int32 year = 1;

  // Month of a year. Must be from 1 to 12, or 0 to specify a year without a
  // month and day.
  int32 month = 2;

  // Day of a month. Must be from 1 to 31 and valid for the year and month, or 0
  // to specify a year by itself or a year and month where the day isn't
  // significant.
  int32 day = 3;
}"#
        }
        CommonSchema::DateTime => {
            r#"// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";

package google.type;

import "google/protobuf/duration.proto";

option cc_enable_arenas = true;
option go_package = "google.golang.org/genproto/googleapis/type/datetime;datetime";
option java_multiple_files = true;
option java_outer_classname = "DateTimeProto";
option java_package = "com.google.type";
option objc_class_prefix = "GTP";

// Represents civil time (or occasionally physical time).
//
// This type can represent a civil time in one of a few possible ways:
//
//  * When utc_offset is set and time_zone is unset: a civil time on a calendar
//    day with a particular offset from UTC.
//  * When time_zone is set and utc_offset is unset: a civil time on a calendar
//    day in a particular time zone.
//  * When neither time_zone nor utc_offset is set: a civil time on a calendar
//    day in local time.
//
// The date is relative to the Proleptic Gregorian Calendar.
//
// If year is 0, the DateTime is considered not to have a specific year. month
// and day must have valid, non-zero values.
//
// This type may also be used to represent a physical time if all the date and
// time fields are set and either case of the `time_offset` oneof is set.
// Consider using `Timestamp` message for physical time instead. If your use
// case also would like to store the user's timezone, that can be done in
// another field.
//
// This type is more flexible than some applications may want. Make sure to
// document and validate your application's limitations.
message DateTime {
  // Optional. Year of date. Must be from 1 to 9999, or 0 if specifying a
  // datetime without a year.
  int32 year = 1;

  // Required. Month of year. Must be from 1 to 12.
  int32 month = 2;

  // Required. Day of month. Must be from 1 to 31 and valid for the year and
  // month.
  int32 day = 3;

  // Required. Hours of day in 24 hour format. Should be from 0 to 23. An API
  // may choose to allow the value "24:00:00" for scenarios like business
  // closing time.
  int32 hours = 4;

  // Required. Minutes of hour of day. Must be from 0 to 59.
  int32 minutes = 5;

  // Required. Seconds of minutes of the time. Must normally be from 0 to 59. An
  // API may allow the value 60 if it allows leap-seconds.
  int32 seconds = 6;

  // Required. Fractions of seconds in nanoseconds. Must be from 0 to
  // 999,999,999.
  int32 nanos = 7;

  // Optional. Specifies either the UTC offset or the time zone of the DateTime.
  // Choose carefully between them, considering that time zone data may change
  // in the future (for example, a country modifies their DST start/end dates,
  // and future DateTimes in the affected range had already been stored).
  // If omitted, the DateTime is considered to be in local time.
  oneof time_offset {
    // UTC offset. Must be whole seconds, between -18 hours and +18 hours.
    // For example, a UTC offset of -4:00 would be represented as
    // { seconds: -14400 }.
    google.protobuf.Duration utc_offset = 8;

    // Time zone.
    TimeZone time_zone = 9;
  }
}

// Represents a time zone from the
// [IANA Time Zone Database](https://www.iana.org/time-zones).
message TimeZone {
  // IANA Time Zone Database time zone, e.g. "America/New_York".
  string id = 1;

  // Optional. IANA Time Zone Database version number, e.g. "2019a".
  string version = 2;
}"#
        }
        CommonSchema::DayOfWeek => {
            r#"// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";

package google.type;

option go_package = "google.golang.org/genproto/googleapis/type/dayofweek;dayofweek";
option java_multiple_files = true;
option java_outer_classname = "DayOfWeekProto";
option java_package = "com.google.type";
option objc_class_prefix = "GTP";

// Represents a day of the week.
enum DayOfWeek {
  // The day of the week is unspecified.
  DAY_OF_WEEK_UNSPECIFIED = 0;

  // Monday
  MONDAY = 1;

  // Tuesday
  TUESDAY = 2;

  // Wednesday
  WEDNESDAY = 3;

  // Thursday
  THURSDAY = 4;

  // Friday
  FRIDAY = 5;

  // Saturday
  SATURDAY = 6;

  // Sunday
  SUNDAY = 7;
}"#
        }
        CommonSchema::Decimal => {
            r#"// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";

package google.type;

option cc_enable_arenas = true;
option go_package = "google.golang.org/genproto/googleapis/type/decimal;decimal";
option java_multiple_files = true;
option java_outer_classname = "DecimalProto";
option java_package = "com.google.type";
option objc_class_prefix = "GTP";

// A representation of a decimal value, such as 2.5. Clients may convert values
// into language-native decimal formats, such as Java's [BigDecimal][] or
// Python's [decimal.Decimal][].
//
// [BigDecimal]:
// https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/math/BigDecimal.html
// [decimal.Decimal]: https://docs.python.org/3/library/decimal.html
message Decimal {
  // The decimal value, as a string.
  //
  // The string representation consists of an optional sign, `+` (`U+002B`)
  // or `-` (`U+002D`), followed by a sequence of zero or more decimal digits
  // ("the integer"), optionally followed by a fraction, optionally followed
  // by an exponent.
  //
  // The fraction consists of a decimal point followed by zero or more decimal
  // digits. The string must contain at least one digit in either the integer
  // or the fraction. The number formed by the sign, the integer and the
  // fraction is referred to as the significand.
  //
  // The exponent consists of the character `e` (`U+0065`) or `E` (`U+0045`)
  // followed by one or more decimal digits.
  //
  // Services **should** normalize decimal values before storing them by:
  //
  //   - Removing an explicitly-provided `+` sign (`+2.5` -> `2.5`).
  //   - Replacing a zero-length integer value with `0` (`.5` -> `0.5`).
  //   - Coercing the exponent character to lower-case (`2.5E8` -> `2.5e8`).
  //   - Removing an explicitly-provided zero exponent (`2.5e0` -> `2.5`).
  //
  // Services **may** perform additional normalization based on its own needs
  // and the internal decimal implementation selected, such as shifting the
  // decimal point and exponent value together (example: `2.5e-1` <-> `0.25`).
  // Additionally, services **may** preserve trailing zeroes in the fraction
  // to indicate increased precision, but are not required to do so.
  //
  // Note that only the `.` character is supported to divide the integer
  // and the fraction; `,` **should not** be supported regardless of locale.
  // Additionally, thousand separators **should not** be supported. If a
  // service does support them, values **must** be normalized.
  //
  // The ENBF grammar is:
  //
  //     DecimalString =
  //       [Sign] Significand [Exponent];
  //
  //     Sign = '+' | '-';
  //
  //     Significand =
  //       Digits ['.'] [Digits] | [Digits] '.' Digits;
  //
  //     Exponent = ('e' | 'E') [Sign] Digits;
  //
  //     Digits = { '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' };
  //
  // Services **should** clearly document the range of supported values, the
  // maximum supported precision (total number of digits), and, if applicable,
  // the scale (number of digits after the decimal point), as well as how it
  // behaves when receiving out-of-bounds values.
  //
  // Services **may** choose to accept values passed as input even when the
  // value has a higher precision or scale than the service supports, and
  // **should** round the value to fit the supported scale. Alternatively, the
  // service **may** error with `400 Bad Request` (`INVALID_ARGUMENT` in gRPC)
  // if precision would be lost.
  //
  // Services **should** error with `400 Bad Request` (`INVALID_ARGUMENT` in
  // gRPC) if the service receives a value outside of the supported range.
  string value = 1;
}"#
        }
        CommonSchema::Duration => {
            r#"
// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

syntax = "proto3";

package google.protobuf;

option csharp_namespace = "Google.Protobuf.WellKnownTypes";
option cc_enable_arenas = true;
option go_package = "google.golang.org/protobuf/types/known/durationpb";
option java_package = "com.google.protobuf";
option java_outer_classname = "DurationProto";
option java_multiple_files = true;
option objc_class_prefix = "GPB";

// A Duration represents a signed, fixed-length span of time represented
// as a count of seconds and fractions of seconds at nanosecond
// resolution. It is independent of any calendar and concepts like "day"
// or "month". It is related to Timestamp in that the difference between
// two Timestamp values is a Duration and it can be added or subtracted
// from a Timestamp. Range is approximately +-10,000 years.
//
// # Examples
//
// Example 1: Compute Duration from two Timestamps in pseudo code.
//
//     Timestamp start = ...;
//     Timestamp end = ...;
//     Duration duration = ...;
//
//     duration.seconds = end.seconds - start.seconds;
//     duration.nanos = end.nanos - start.nanos;
//
//     if (duration.seconds < 0 && duration.nanos > 0) {
//       duration.seconds += 1;
//       duration.nanos -= 1000000000;
//     } else if (duration.seconds > 0 && duration.nanos < 0) {
//       duration.seconds -= 1;
//       duration.nanos += 1000000000;
//     }
//
// Example 2: Compute Timestamp from Timestamp + Duration in pseudo code.
//
//     Timestamp start = ...;
//     Duration duration = ...;
//     Timestamp end = ...;
//
//     end.seconds = start.seconds + duration.seconds;
//     end.nanos = start.nanos + duration.nanos;
//
//     if (end.nanos < 0) {
//       end.seconds -= 1;
//       end.nanos += 1000000000;
//     } else if (end.nanos >= 1000000000) {
//       end.seconds += 1;
//       end.nanos -= 1000000000;
//     }
//
// Example 3: Compute Duration from datetime.timedelta in Python.
//
//     td = datetime.timedelta(days=3, minutes=10)
//     duration = Duration()
//     duration.FromTimedelta(td)
//
// # JSON Mapping
//
// In JSON format, the Duration type is encoded as a string rather than an
// object, where the string ends in the suffix "s" (indicating seconds) and
// is preceded by the number of seconds, with nanoseconds expressed as
// fractional seconds. For example, 3 seconds with 0 nanoseconds should be
// encoded in JSON format as "3s", while 3 seconds and 1 nanosecond should
// be expressed in JSON format as "3.000000001s", and 3 seconds and 1
// microsecond should be expressed in JSON format as "3.000001s".
//
//
message Duration {
  // Signed seconds of the span of time. Must be from -315,576,000,000
  // to +315,576,000,000 inclusive. Note: these bounds are computed from:
  // 60 sec/min * 60 min/hr * 24 hr/day * 365.25 days/year * 10000 years
  int64 seconds = 1;

  // Signed fractions of a second at nanosecond resolution of the span
  // of time. Durations less than one second are represented with a 0
  // `seconds` field and a positive or negative `nanos` field. For durations
  // of one second or more, a non-zero value for the `nanos` field must be
  // of the same sign as the `seconds` field. Must be from -999,999,999
  // to +999,999,999 inclusive.
  int32 nanos = 2;
}"#
        }
        CommonSchema::Expr => {
            r#"// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";

package google.type;

option go_package = "google.golang.org/genproto/googleapis/type/expr;expr";
option java_multiple_files = true;
option java_outer_classname = "ExprProto";
option java_package = "com.google.type";
option objc_class_prefix = "GTP";

// Represents a textual expression in the Common Expression Language (CEL)
// syntax. CEL is a C-like expression language. The syntax and semantics of CEL
// are documented at https://github.com/google/cel-spec.
//
// Example (Comparison):
//
//     title: "Summary size limit"
//     description: "Determines if a summary is less than 100 chars"
//     expression: "document.summary.size() < 100"
//
// Example (Equality):
//
//     title: "Requestor is owner"
//     description: "Determines if requestor is the document owner"
//     expression: "document.owner == request.auth.claims.email"
//
// Example (Logic):
//
//     title: "Public documents"
//     description: "Determine whether the document should be publicly visible"
//     expression: "document.type != 'private' && document.type != 'internal'"
//
// Example (Data Manipulation):
//
//     title: "Notification string"
//     description: "Create a notification string with a timestamp."
//     expression: "'New message received at ' + string(document.create_time)"
//
// The exact variables and functions that may be referenced within an expression
// are determined by the service that evaluates it. See the service
// documentation for additional information.
message Expr {
  // Textual representation of an expression in Common Expression Language
  // syntax.
  string expression = 1;

  // Optional. Title for the expression, i.e. a short string describing
  // its purpose. This can be used e.g. in UIs which allow to enter the
  // expression.
  string title = 2;

  // Optional. Description of the expression. This is a longer text which
  // describes the expression, e.g. when hovered over it in a UI.
  string description = 3;

  // Optional. String indicating the location of the expression for error
  // reporting, e.g. a file name and a position in the file.
  string location = 4;
}
        "#
        }
        CommonSchema::Fraction => {
            r#"// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";

package google.type;

option go_package = "google.golang.org/genproto/googleapis/type/fraction;fraction";
option java_multiple_files = true;
option java_outer_classname = "FractionProto";
option java_package = "com.google.type";
option objc_class_prefix = "GTP";

// Represents a fraction in terms of a numerator divided by a denominator.
message Fraction {
  // The numerator in the fraction, e.g. 2 in 2/3.
  int64 numerator = 1;

  // The value by which the numerator is divided, e.g. 3 in 2/3. Must be
  // positive.
  int64 denominator = 2;
}"#
        }
        CommonSchema::Interval => {
            r#"// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";

package google.type;

import "google/protobuf/timestamp.proto";

option cc_enable_arenas = true;
option go_package = "google.golang.org/genproto/googleapis/type/interval;interval";
option java_multiple_files = true;
option java_outer_classname = "IntervalProto";
option java_package = "com.google.type";
option objc_class_prefix = "GTP";

// Represents a time interval, encoded as a Timestamp start (inclusive) and a
// Timestamp end (exclusive).
//
// The start must be less than or equal to the end.
// When the start equals the end, the interval is empty (matches no time).
// When both start and end are unspecified, the interval matches any time.
message Interval {
  // Optional. Inclusive start of the interval.
  //
  // If specified, a Timestamp matching this interval will have to be the same
  // or after the start.
  google.protobuf.Timestamp start_time = 1;

  // Optional. Exclusive end of the interval.
  //
  // If specified, a Timestamp matching this interval will have to be before the
  // end.
  google.protobuf.Timestamp end_time = 2;
}
        "#
        }
        CommonSchema::LatLng => {
            r#"// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";

package google.type;

option cc_enable_arenas = true;
option go_package = "google.golang.org/genproto/googleapis/type/latlng;latlng";
option java_multiple_files = true;
option java_outer_classname = "LatLngProto";
option java_package = "com.google.type";
option objc_class_prefix = "GTP";

// An object that represents a latitude/longitude pair. This is expressed as a
// pair of doubles to represent degrees latitude and degrees longitude. Unless
// specified otherwise, this must conform to the
// <a href="http://www.unoosa.org/pdf/icg/2012/template/WGS_84.pdf">WGS84
// standard</a>. Values must be within normalized ranges.
message LatLng {
  // The latitude in degrees. It must be in the range [-90.0, +90.0].
  double latitude = 1;

  // The longitude in degrees. It must be in the range [-180.0, +180.0].
  double longitude = 2;
}"#
        }
        CommonSchema::LocalizedText => {
            r#"// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";

package google.type;

option cc_enable_arenas = true;
option go_package = "google.golang.org/genproto/googleapis/type/localized_text;localized_text";
option java_multiple_files = true;
option java_outer_classname = "LocalizedTextProto";
option java_package = "com.google.type";
option objc_class_prefix = "GTP";

// Localized variant of a text in a particular language.
message LocalizedText {
  // Localized string in the language corresponding to `language_code' below.
  string text = 1;

  // The text's BCP-47 language code, such as "en-US" or "sr-Latn".
  //
  // For more information, see
  // http://www.unicode.org/reports/tr35/#Unicode_locale_identifier.
  string language_code = 2;
}"#
        }
        CommonSchema::Money => {
            r#"// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";

package google.type;

option cc_enable_arenas = true;
option go_package = "google.golang.org/genproto/googleapis/type/money;money";
option java_multiple_files = true;
option java_outer_classname = "MoneyProto";
option java_package = "com.google.type";
option objc_class_prefix = "GTP";

// Represents an amount of money with its currency type.
message Money {
  // The three-letter currency code defined in ISO 4217.
  string currency_code = 1;

  // The whole units of the amount.
  // For example if `currencyCode` is `"USD"`, then 1 unit is one US dollar.
  int64 units = 2;

  // Number of nano (10^-9) units of the amount.
  // The value must be between -999,999,999 and +999,999,999 inclusive.
  // If `units` is positive, `nanos` must be positive or zero.
  // If `units` is zero, `nanos` can be positive, zero, or negative.
  // If `units` is negative, `nanos` must be negative or zero.
  // For example $-1.75 is represented as `units`=-1 and `nanos`=-750,000,000.
  int32 nanos = 3;
}"#
        }
        CommonSchema::Month => {
            r#"// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";

package google.type;

option go_package = "google.golang.org/genproto/googleapis/type/month;month";
option java_multiple_files = true;
option java_outer_classname = "MonthProto";
option java_package = "com.google.type";
option objc_class_prefix = "GTP";

// Represents a month in the Gregorian calendar.
enum Month {
  // The unspecified month.
  MONTH_UNSPECIFIED = 0;

  // The month of January.
  JANUARY = 1;

  // The month of February.
  FEBRUARY = 2;

  // The month of March.
  MARCH = 3;

  // The month of April.
  APRIL = 4;

  // The month of May.
  MAY = 5;

  // The month of June.
  JUNE = 6;

  // The month of July.
  JULY = 7;

  // The month of August.
  AUGUST = 8;

  // The month of September.
  SEPTEMBER = 9;

  // The month of October.
  OCTOBER = 10;

  // The month of November.
  NOVEMBER = 11;

  // The month of December.
  DECEMBER = 12;
}"#
        }
        CommonSchema::PhoneNumber => {
            r#"// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";

package google.type;

option cc_enable_arenas = true;
option go_package = "google.golang.org/genproto/googleapis/type/phone_number;phone_number";
option java_multiple_files = true;
option java_outer_classname = "PhoneNumberProto";
option java_package = "com.google.type";
option objc_class_prefix = "GTP";

// An object representing a phone number, suitable as an API wire format.
//
// This representation:
//
//  - should not be used for locale-specific formatting of a phone number, such
//    as "+1 (650) 253-0000 ext. 123"
//
//  - is not designed for efficient storage
//  - may not be suitable for dialing - specialized libraries (see references)
//    should be used to parse the number for that purpose
//
// To do something meaningful with this number, such as format it for various
// use-cases, convert it to an `i18n.phonenumbers.PhoneNumber` object first.
//
// For instance, in Java this would be:
//
//    com.google.type.PhoneNumber wireProto =
//        com.google.type.PhoneNumber.newBuilder().build();
//    com.google.i18n.phonenumbers.Phonenumber.PhoneNumber phoneNumber =
//        PhoneNumberUtil.getInstance().parse(wireProto.getE164Number(), "ZZ");
//    if (!wireProto.getExtension().isEmpty()) {
//      phoneNumber.setExtension(wireProto.getExtension());
//    }
//
//  Reference(s):
//   - https://github.com/google/libphonenumber
message PhoneNumber {
  // An object representing a short code, which is a phone number that is
  // typically much shorter than regular phone numbers and can be used to
  // address messages in MMS and SMS systems, as well as for abbreviated dialing
  // (e.g. "Text 611 to see how many minutes you have remaining on your plan.").
  //
  // Short codes are restricted to a region and are not internationally
  // dialable, which means the same short code can exist in different regions,
  // with different usage and pricing, even if those regions share the same
  // country calling code (e.g. US and CA).
  message ShortCode {
    // Required. The BCP-47 region code of the location where calls to this
    // short code can be made, such as "US" and "BB".
    //
    // Reference(s):
    //  - http://www.unicode.org/reports/tr35/#unicode_region_subtag
    string region_code = 1;

    // Required. The short code digits, without a leading plus ('+') or country
    // calling code, e.g. "611".
    string number = 2;
  }

  // Required.  Either a regular number, or a short code.  New fields may be
  // added to the oneof below in the future, so clients should ignore phone
  // numbers for which none of the fields they coded against are set.
  oneof kind {
    // The phone number, represented as a leading plus sign ('+'), followed by a
    // phone number that uses a relaxed ITU E.164 format consisting of the
    // country calling code (1 to 3 digits) and the subscriber number, with no
    // additional spaces or formatting, e.g.:
    //  - correct: "+15552220123"
    //  - incorrect: "+1 (555) 222-01234 x123".
    //
    // The ITU E.164 format limits the latter to 12 digits, but in practice not
    // all countries respect that, so we relax that restriction here.
    // National-only numbers are not allowed.
    //
    // References:
    //  - https://www.itu.int/rec/T-REC-E.164-201011-I
    //  - https://en.wikipedia.org/wiki/E.164.
    //  - https://en.wikipedia.org/wiki/List_of_country_calling_codes
    string e164_number = 1;

    // A short code.
    //
    // Reference(s):
    //  - https://en.wikipedia.org/wiki/Short_code
    ShortCode short_code = 2;
  }

  // The phone number's extension. The extension is not standardized in ITU
  // recommendations, except for being defined as a series of numbers with a
  // maximum length of 40 digits. Other than digits, some other dialing
  // characters such as ',' (indicating a wait) or '#' may be stored here.
  //
  // Note that no regions currently use extensions with short codes, so this
  // field is normally only set in conjunction with an E.164 number. It is held
  // separately from the E.164 number to allow for short code extensions in the
  // future.
  string extension = 3;
}"#
        }
        CommonSchema::PostalAddress => {
            r#"// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";

package google.type;

option cc_enable_arenas = true;
option go_package = "google.golang.org/genproto/googleapis/type/postaladdress;postaladdress";
option java_multiple_files = true;
option java_outer_classname = "PostalAddressProto";
option java_package = "com.google.type";
option objc_class_prefix = "GTP";

// Represents a postal address, e.g. for postal delivery or payments addresses.
// Given a postal address, a postal service can deliver items to a premise, P.O.
// Box or similar.
// It is not intended to model geographical locations (roads, towns,
// mountains).
//
// In typical usage an address would be created via user input or from importing
// existing data, depending on the type of process.
//
// Advice on address input / editing:
//  - Use an i18n-ready address widget such as
//    https://github.com/google/libaddressinput)
// - Users should not be presented with UI elements for input or editing of
//   fields outside countries where that field is used.
//
// For more guidance on how to use this schema, please see:
// https://support.google.com/business/answer/6397478
message PostalAddress {
  // The schema revision of the `PostalAddress`. This must be set to 0, which is
  // the latest revision.
  //
  // All new revisions **must** be backward compatible with old revisions.
  int32 revision = 1;

  // Required. CLDR region code of the country/region of the address. This
  // is never inferred and it is up to the user to ensure the value is
  // correct. See http://cldr.unicode.org/ and
  // http://www.unicode.org/cldr/charts/30/supplemental/territory_information.html
  // for details. Example: "CH" for Switzerland.
  string region_code = 2;

  // Optional. BCP-47 language code of the contents of this address (if
  // known). This is often the UI language of the input form or is expected
  // to match one of the languages used in the address' country/region, or their
  // transliterated equivalents.
  // This can affect formatting in certain countries, but is not critical
  // to the correctness of the data and will never affect any validation or
  // other non-formatting related operations.
  //
  // If this value is not known, it should be omitted (rather than specifying a
  // possibly incorrect default).
  //
  // Examples: "zh-Hant", "ja", "ja-Latn", "en".
  string language_code = 3;

  // Optional. Postal code of the address. Not all countries use or require
  // postal codes to be present, but where they are used, they may trigger
  // additional validation with other parts of the address (e.g. state/zip
  // validation in the U.S.A.).
  string postal_code = 4;

  // Optional. Additional, country-specific, sorting code. This is not used
  // in most regions. Where it is used, the value is either a string like
  // "CEDEX", optionally followed by a number (e.g. "CEDEX 7"), or just a number
  // alone, representing the "sector code" (Jamaica), "delivery area indicator"
  // (Malawi) or "post office indicator" (e.g. Côte d'Ivoire).
  string sorting_code = 5;

  // Optional. Highest administrative subdivision which is used for postal
  // addresses of a country or region.
  // For example, this can be a state, a province, an oblast, or a prefecture.
  // Specifically, for Spain this is the province and not the autonomous
  // community (e.g. "Barcelona" and not "Catalonia").
  // Many countries don't use an administrative area in postal addresses. E.g.
  // in Switzerland this should be left unpopulated.
  string administrative_area = 6;

  // Optional. Generally refers to the city/town portion of the address.
  // Examples: US city, IT comune, UK post town.
  // In regions of the world where localities are not well defined or do not fit
  // into this structure well, leave locality empty and use address_lines.
  string locality = 7;

  // Optional. Sublocality of the address.
  // For example, this can be neighborhoods, boroughs, districts.
  string sublocality = 8;

  // Unstructured address lines describing the lower levels of an address.
  //
  // Because values in address_lines do not have type information and may
  // sometimes contain multiple values in a single field (e.g.
  // "Austin, TX"), it is important that the line order is clear. The order of
  // address lines should be "envelope order" for the country/region of the
  // address. In places where this can vary (e.g. Japan), address_language is
  // used to make it explicit (e.g. "ja" for large-to-small ordering and
  // "ja-Latn" or "en" for small-to-large). This way, the most specific line of
  // an address can be selected based on the language.
  //
  // The minimum permitted structural representation of an address consists
  // of a region_code with all remaining information placed in the
  // address_lines. It would be possible to format such an address very
  // approximately without geocoding, but no semantic reasoning could be
  // made about any of the address components until it was at least
  // partially resolved.
  //
  // Creating an address only containing a region_code and address_lines, and
  // then geocoding is the recommended way to handle completely unstructured
  // addresses (as opposed to guessing which parts of the address should be
  // localities or administrative areas).
  repeated string address_lines = 9;

  // Optional. The recipient at the address.
  // This field may, under certain circumstances, contain multiline information.
  // For example, it might contain "care of" information.
  repeated string recipients = 10;

  // Optional. The name of the organization at the address.
  string organization = 11;
}"#
        }
        CommonSchema::Quaternion => {
            r#"// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";

package google.type;

option cc_enable_arenas = true;
option go_package = "google.golang.org/genproto/googleapis/type/quaternion;quaternion";
option java_multiple_files = true;
option java_outer_classname = "QuaternionProto";
option java_package = "com.google.type";
option objc_class_prefix = "GTP";

// A quaternion is defined as the quotient of two directed lines in a
// three-dimensional space or equivalently as the quotient of two Euclidean
// vectors (https://en.wikipedia.org/wiki/Quaternion).
//
// Quaternions are often used in calculations involving three-dimensional
// rotations (https://en.wikipedia.org/wiki/Quaternions_and_spatial_rotation),
// as they provide greater mathematical robustness by avoiding the gimbal lock
// problems that can be encountered when using Euler angles
// (https://en.wikipedia.org/wiki/Gimbal_lock).
//
// Quaternions are generally represented in this form:
//
//     w + xi + yj + zk
//
// where x, y, z, and w are real numbers, and i, j, and k are three imaginary
// numbers.
//
// Our naming choice `(x, y, z, w)` comes from the desire to avoid confusion for
// those interested in the geometric properties of the quaternion in the 3D
// Cartesian space. Other texts often use alternative names or subscripts, such
// as `(a, b, c, d)`, `(1, i, j, k)`, or `(0, 1, 2, 3)`, which are perhaps
// better suited for mathematical interpretations.
//
// To avoid any confusion, as well as to maintain compatibility with a large
// number of software libraries, the quaternions represented using the protocol
// buffer below *must* follow the Hamilton convention, which defines `ij = k`
// (i.e. a right-handed algebra), and therefore:
//
//     i^2 = j^2 = k^2 = ijk = −1
//     ij = −ji = k
//     jk = −kj = i
//     ki = −ik = j
//
// Please DO NOT use this to represent quaternions that follow the JPL
// convention, or any of the other quaternion flavors out there.
//
// Definitions:
//
//   - Quaternion norm (or magnitude): `sqrt(x^2 + y^2 + z^2 + w^2)`.
//   - Unit (or normalized) quaternion: a quaternion whose norm is 1.
//   - Pure quaternion: a quaternion whose scalar component (`w`) is 0.
//   - Rotation quaternion: a unit quaternion used to represent rotation.
//   - Orientation quaternion: a unit quaternion used to represent orientation.
//
// A quaternion can be normalized by dividing it by its norm. The resulting
// quaternion maintains the same direction, but has a norm of 1, i.e. it moves
// on the unit sphere. This is generally necessary for rotation and orientation
// quaternions, to avoid rounding errors:
// https://en.wikipedia.org/wiki/Rotation_formalisms_in_three_dimensions
//
// Note that `(x, y, z, w)` and `(-x, -y, -z, -w)` represent the same rotation,
// but normalization would be even more useful, e.g. for comparison purposes, if
// it would produce a unique representation. It is thus recommended that `w` be
// kept positive, which can be achieved by changing all the signs when `w` is
// negative.
//
message Quaternion {
  // The x component.
  double x = 1;

  // The y component.
  double y = 2;

  // The z component.
  double z = 3;

  // The scalar component.
  double w = 4;
}"#
        }
        CommonSchema::Timestamp => {
            r#"// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

syntax = "proto3";

package google.protobuf;

option csharp_namespace = "Google.Protobuf.WellKnownTypes";
option cc_enable_arenas = true;
option go_package = "google.golang.org/protobuf/types/known/timestamppb";
option java_package = "com.google.protobuf";
option java_outer_classname = "TimestampProto";
option java_multiple_files = true;
option objc_class_prefix = "GPB";

// A Timestamp represents a point in time independent of any time zone or local
// calendar, encoded as a count of seconds and fractions of seconds at
// nanosecond resolution. The count is relative to an epoch at UTC midnight on
// January 1, 1970, in the proleptic Gregorian calendar which extends the
// Gregorian calendar backwards to year one.
//
// All minutes are 60 seconds long. Leap seconds are "smeared" so that no leap
// second table is needed for interpretation, using a [24-hour linear
// smear](https://developers.google.com/time/smear).
//
// The range is from 0001-01-01T00:00:00Z to 9999-12-31T23:59:59.999999999Z. By
// restricting to that range, we ensure that we can convert to and from [RFC
// 3339](https://www.ietf.org/rfc/rfc3339.txt) date strings.
//
// # Examples
//
// Example 1: Compute Timestamp from POSIX `time()`.
//
//     Timestamp timestamp;
//     timestamp.set_seconds(time(NULL));
//     timestamp.set_nanos(0);
//
// Example 2: Compute Timestamp from POSIX `gettimeofday()`.
//
//     struct timeval tv;
//     gettimeofday(&tv, NULL);
//
//     Timestamp timestamp;
//     timestamp.set_seconds(tv.tv_sec);
//     timestamp.set_nanos(tv.tv_usec * 1000);
//
// Example 3: Compute Timestamp from Win32 `GetSystemTimeAsFileTime()`.
//
//     FILETIME ft;
//     GetSystemTimeAsFileTime(&ft);
//     UINT64 ticks = (((UINT64)ft.dwHighDateTime) << 32) | ft.dwLowDateTime;
//
//     // A Windows tick is 100 nanoseconds. Windows epoch 1601-01-01T00:00:00Z
//     // is 11644473600 seconds before Unix epoch 1970-01-01T00:00:00Z.
//     Timestamp timestamp;
//     timestamp.set_seconds((INT64) ((ticks / 10000000) - 11644473600LL));
//     timestamp.set_nanos((INT32) ((ticks % 10000000) * 100));
//
// Example 4: Compute Timestamp from Java `System.currentTimeMillis()`.
//
//     long millis = System.currentTimeMillis();
//
//     Timestamp timestamp = Timestamp.newBuilder().setSeconds(millis / 1000)
//         .setNanos((int) ((millis % 1000) * 1000000)).build();
//
//
// Example 5: Compute Timestamp from Java `Instant.now()`.
//
//     Instant now = Instant.now();
//
//     Timestamp timestamp =
//         Timestamp.newBuilder().setSeconds(now.getEpochSecond())
//             .setNanos(now.getNano()).build();
//
//
// Example 6: Compute Timestamp from current time in Python.
//
//     timestamp = Timestamp()
//     timestamp.GetCurrentTime()
//
// # JSON Mapping
//
// In JSON format, the Timestamp type is encoded as a string in the
// [RFC 3339](https://www.ietf.org/rfc/rfc3339.txt) format. That is, the
// format is "{year}-{month}-{day}T{hour}:{min}:{sec}[.{frac_sec}]Z"
// where {year} is always expressed using four digits while {month}, {day},
// {hour}, {min}, and {sec} are zero-padded to two digits each. The fractional
// seconds, which can go up to 9 digits (i.e. up to 1 nanosecond resolution),
// are optional. The "Z" suffix indicates the timezone ("UTC"); the timezone
// is required. A proto3 JSON serializer should always use UTC (as indicated by
// "Z") when printing the Timestamp type and a proto3 JSON parser should be
// able to accept both UTC and other timezones (as indicated by an offset).
//
// For example, "2017-01-15T01:30:15.01Z" encodes 15.01 seconds past
// 01:30 UTC on January 15, 2017.
//
// In JavaScript, one can convert a Date object to this format using the
// standard
// [toISOString()](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/toISOString)
// method. In Python, a standard `datetime.datetime` object can be converted
// to this format using
// [`strftime`](https://docs.python.org/2/library/time.html#time.strftime) with
// the time format spec '%Y-%m-%dT%H:%M:%S.%fZ'. Likewise, in Java, one can use
// the Joda Time's [`ISODateTimeFormat.dateTime()`](
// http://www.joda.org/joda-time/apidocs/org/joda/time/format/ISODateTimeFormat.html#dateTime%2D%2D
// ) to obtain a formatter capable of generating timestamps in this format.
//
//
message Timestamp {
  // Represents seconds of UTC time since Unix epoch
  // 1970-01-01T00:00:00Z. Must be from 0001-01-01T00:00:00Z to
  // 9999-12-31T23:59:59Z inclusive.
  int64 seconds = 1;

  // Non-negative fractions of a second at nanosecond resolution. Negative
  // second values with fractions must still have non-negative nanos values
  // that count forward in time. Must be from 0 to 999,999,999
  // inclusive.
  int32 nanos = 2;
}"#
        }
        CommonSchema::TimeOfDay => {
            r#"// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";

package google.type;

option cc_enable_arenas = true;
option go_package = "google.golang.org/genproto/googleapis/type/timeofday;timeofday";
option java_multiple_files = true;
option java_outer_classname = "TimeOfDayProto";
option java_package = "com.google.type";
option objc_class_prefix = "GTP";

// Represents a time of day. The date and time zone are either not significant
// or are specified elsewhere. An API may choose to allow leap seconds. Related
// types are [google.type.Date][google.type.Date] and
// `google.protobuf.Timestamp`.
message TimeOfDay {
  // Hours of day in 24 hour format. Should be from 0 to 23. An API may choose
  // to allow the value "24:00:00" for scenarios like business closing time.
  int32 hours = 1;

  // Minutes of hour of day. Must be from 0 to 59.
  int32 minutes = 2;

  // Seconds of minutes of the time. Must normally be from 0 to 59. An API may
  // allow the value 60 if it allows leap-seconds.
  int32 seconds = 3;

  // Fractions of seconds in nanoseconds. Must be from 0 to 999,999,999.
  int32 nanos = 4;
}"#
        }
        CommonSchema::Type => {
            r#"type: google.api.Service
config_version: 3
name: type.googleapis.com
title: Common Types

types:
- name: google.type.Color
- name: google.type.Date
- name: google.type.DateTime
- name: google.type.Decimal
- name: google.type.Expr
- name: google.type.Fraction
- name: google.type.Interval
- name: google.type.LatLng
- name: google.type.LocalizedText
- name: google.type.Money
- name: google.type.PhoneNumber
- name: google.type.PostalAddress
- name: google.type.Quaternion
- name: google.type.TimeOfDay

enums:
- name: google.type.CalendarPeriod
- name: google.type.DayOfWeek
- name: google.type.Month

documentation:
  summary: Defines common types for Google APIs.
  overview: |-
    # Google Common Types
    This package contains definitions of common types for Google APIs.
    All types defined in this package are suitable for different APIs to
    exchange data, and will never break binary compatibility. They should
    have design quality comparable to major programming languages like
    Java and C#.
    NOTE: Some common types are defined in the package `google.protobuf`
    as they are directly supported by Protocol Buffers compiler and
    runtime. Those types are called Well-Known Types."#
        }
        CommonSchema::Wrappers => {
            r#"// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// Wrappers for primitive (non-message) types. These types are useful
// for embedding primitives in the `google.protobuf.Any` type and for places
// where we need to distinguish between the absence of a primitive
// typed field and its default value.
//
// These wrappers have no meaningful use within repeated fields as they lack
// the ability to detect presence on individual elements.
// These wrappers have no meaningful use within a map or a oneof since
// individual entries of a map or fields of a oneof can already detect presence.

syntax = "proto3";

package google.protobuf;

option csharp_namespace = "Google.Protobuf.WellKnownTypes";
option cc_enable_arenas = true;
option go_package = "google.golang.org/protobuf/types/known/wrapperspb";
option java_package = "com.google.protobuf";
option java_outer_classname = "WrappersProto";
option java_multiple_files = true;
option objc_class_prefix = "GPB";

// Wrapper message for `double`.
//
// The JSON representation for `DoubleValue` is JSON number.
message DoubleValue {
  // The double value.
  double value = 1;
}

// Wrapper message for `float`.
//
// The JSON representation for `FloatValue` is JSON number.
message FloatValue {
  // The float value.
  float value = 1;
}

// Wrapper message for `int64`.
//
// The JSON representation for `Int64Value` is JSON string.
message Int64Value {
  // The int64 value.
  int64 value = 1;
}

// Wrapper message for `uint64`.
//
// The JSON representation for `UInt64Value` is JSON string.
message UInt64Value {
  // The uint64 value.
  uint64 value = 1;
}

// Wrapper message for `int32`.
//
// The JSON representation for `Int32Value` is JSON number.
message Int32Value {
  // The int32 value.
  int32 value = 1;
}

// Wrapper message for `uint32`.
//
// The JSON representation for `UInt32Value` is JSON number.
message UInt32Value {
  // The uint32 value.
  uint32 value = 1;
}

// Wrapper message for `bool`.
//
// The JSON representation for `BoolValue` is JSON `true` and `false`.
message BoolValue {
  // The bool value.
  bool value = 1;
}

// Wrapper message for `string`.
//
// The JSON representation for `StringValue` is JSON string.
message StringValue {
  // The string value.
  string value = 1;
}

// Wrapper message for `bytes`.
//
// The JSON representation for `BytesValue` is JSON string.
message BytesValue {
  // The bytes value.
  bytes value = 1;
}"#
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::proto_common_types::{get_schema, get_schemas, CommonSchema, CommonType};

    #[test]
    fn test_get_schemas() {
        assert_eq!(get_schemas(CommonType::CalendarPeriod).len(), 1);
        assert_eq!(get_schemas(CommonType::Color).len(), 2);
        assert_eq!(get_schemas(CommonType::Date).len(), 1);
        assert_eq!(get_schemas(CommonType::DateTime).len(), 2);
        assert_eq!(get_schemas(CommonType::DayOfWeek).len(), 1);
        assert_eq!(get_schemas(CommonType::Decimal).len(), 1);
        assert_eq!(get_schemas(CommonType::Expr).len(), 1);
        assert_eq!(get_schemas(CommonType::Fraction).len(), 1);
        assert_eq!(get_schemas(CommonType::Interval).len(), 2);
        assert_eq!(get_schemas(CommonType::LatLng).len(), 1);
        assert_eq!(get_schemas(CommonType::LocalizedText).len(), 1);
        assert_eq!(get_schemas(CommonType::Money).len(), 1);
        assert_eq!(get_schemas(CommonType::PhoneNumber).len(), 1);
        assert_eq!(get_schemas(CommonType::PostalAddress).len(), 1);
        assert_eq!(get_schemas(CommonType::Quaternion).len(), 1);
        assert_eq!(get_schemas(CommonType::TimeOfDay).len(), 1);
        assert_eq!(get_schemas(CommonType::Type).len(), 1);
    }

    #[test]
    fn test_get_schema() {
        assert_eq!(
            get_schema(&CommonSchema::CalendarPeriod).starts_with("// Copyright 2021 Google LLC"),
            true
        );
        assert_eq!(
            get_schema(&CommonSchema::Color).starts_with("// Copyright 2021 Google LLC"),
            true
        );
        assert_eq!(
            get_schema(&CommonSchema::Date).starts_with("// Copyright 2021 Google LLC"),
            true
        );
        assert_eq!(
            get_schema(&CommonSchema::DateTime).starts_with("// Copyright 2021 Google LLC"),
            true
        );
        assert_eq!(
            get_schema(&CommonSchema::DayOfWeek).starts_with("// Copyright 2021 Google LLC"),
            true
        );
        assert_eq!(
            get_schema(&CommonSchema::Decimal).starts_with("// Copyright 2021 Google LLC"),
            true
        );
        assert_eq!(
            get_schema(&CommonSchema::Expr).starts_with("// Copyright 2021 Google LLC"),
            true
        );
        assert_eq!(
            get_schema(&CommonSchema::Fraction).starts_with("// Copyright 2021 Google LLC"),
            true
        );
        assert_eq!(
            get_schema(&CommonSchema::Interval).starts_with("// Copyright 2021 Google LLC"),
            true
        );
        assert_eq!(
            get_schema(&CommonSchema::LatLng).starts_with("// Copyright 2021 Google LLC"),
            true
        );
        assert_eq!(
            get_schema(&CommonSchema::LocalizedText).starts_with("// Copyright 2021 Google LLC"),
            true
        );
        assert_eq!(
            get_schema(&CommonSchema::Money).starts_with("// Copyright 2021 Google LLC"),
            true
        );
        assert_eq!(
            get_schema(&CommonSchema::Month).starts_with("// Copyright 2021 Google LLC"),
            true
        );
        assert_eq!(
            get_schema(&CommonSchema::PhoneNumber).starts_with("// Copyright 2021 Google LLC"),
            true
        );
        assert_eq!(
            get_schema(&CommonSchema::PostalAddress).starts_with("// Copyright 2021 Google LLC"),
            true
        );
        assert_eq!(
            get_schema(&CommonSchema::Quaternion).starts_with("// Copyright 2021 Google LLC"),
            true
        );
        assert_eq!(
            get_schema(&CommonSchema::TimeOfDay).starts_with("// Copyright 2021 Google LLC"),
            true
        );
        assert_eq!(
            get_schema(&CommonSchema::Type).starts_with("type: google.api.Service"),
            true
        );
    }
}
