use std::collections::HashSet;

/// Adds the schema of the common type imports
pub(crate) fn add_common_files(imports: &Vec<String>, files: &mut HashSet<String>) {
    for import in imports {
        if let Some(common_schema) = is_common_import(import) {
            files.insert(String::from(get_schema(&common_schema)));
            continue;
        }
        if let Some(common_type) = is_common_type_import(import) {
            for common_schema in get_schemas(common_type) {
                files.insert(String::from(get_schema(common_schema)));
            }
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
    Any,
    Api,
    CalendarPeriod,
    Color,
    Date,
    DateTime,
    DayOfWeek,
    Decimal,
    Descriptor,
    Duration,
    Empty,
    Expr,
    FieldMask,
    Fraction,
    Interval,
    LatLng,
    LocalizedText,
    Money,
    Month,
    PhoneNumber,
    PostalAddress,
    Quaternion,
    SourceContext,
    Struct,
    TimeOfDay,
    Timestamp,
    Type,
    Wrappers,
}

fn is_common_import(import: &str) -> Option<CommonSchema> {
    match import {
        "google/protobuf/any.proto" => Some(CommonSchema::Any),
        "google/protobuf/api.proto" => Some(CommonSchema::Api),
        "google/protobuf/descriptor.proto" => Some(CommonSchema::Descriptor),
        "google/protobuf/duration.proto" => Some(CommonSchema::Duration),
        "google/protobuf/empty.proto" => Some(CommonSchema::Empty),
        "google/protobuf/field_mask.proto" => Some(CommonSchema::FieldMask),
        "google/protobuf/source_context.proto" => Some(CommonSchema::SourceContext),
        "google/protobuf/struct.proto" => Some(CommonSchema::Struct),
        "google/protobuf/timestamp.proto" => Some(CommonSchema::Timestamp),
        "google/protobuf/type.proto" => Some(CommonSchema::Type),
        "google/protobuf/wrappers.proto" => Some(CommonSchema::Wrappers),
        _ => None,
    }
}

fn is_common_type_import(import: &str) -> Option<CommonType> {
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
        CommonSchema::Any => {
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

option go_package = "google.golang.org/protobuf/types/known/anypb";
option java_package = "com.google.protobuf";
option java_outer_classname = "AnyProto";
option java_multiple_files = true;
option objc_class_prefix = "GPB";
option csharp_namespace = "Google.Protobuf.WellKnownTypes";

// `Any` contains an arbitrary serialized protocol buffer message along with a
// URL that describes the type of the serialized message.
//
// Protobuf library provides support to pack/unpack Any values in the form
// of utility functions or additional generated methods of the Any type.
//
// Example 1: Pack and unpack a message in C++.
//
//     Foo foo = ...;
//     Any any;
//     any.PackFrom(foo);
//     ...
//     if (any.UnpackTo(&foo)) {
//       ...
//     }
//
// Example 2: Pack and unpack a message in Java.
//
//     Foo foo = ...;
//     Any any = Any.pack(foo);
//     ...
//     if (any.is(Foo.class)) {
//       foo = any.unpack(Foo.class);
//     }
//     // or ...
//     if (any.isSameTypeAs(Foo.getDefaultInstance())) {
//       foo = any.unpack(Foo.getDefaultInstance());
//     }
//
// Example 3: Pack and unpack a message in Python.
//
//     foo = Foo(...)
//     any = Any()
//     any.Pack(foo)
//     ...
//     if any.Is(Foo.DESCRIPTOR):
//       any.Unpack(foo)
//       ...
//
// Example 4: Pack and unpack a message in Go
//
//      foo := &pb.Foo{...}
//      any, err := anypb.New(foo)
//      if err != nil {
//        ...
//      }
//      ...
//      foo := &pb.Foo{}
//      if err := any.UnmarshalTo(foo); err != nil {
//        ...
//      }
//
// The pack methods provided by protobuf library will by default use
// 'type.googleapis.com/full.type.name' as the type URL and the unpack
// methods only use the fully qualified type name after the last '/'
// in the type URL, for example "foo.bar.com/x/y.z" will yield type
// name "y.z".
//
// JSON
//
// The JSON representation of an `Any` value uses the regular
// representation of the deserialized, embedded message, with an
// additional field `@type` which contains the type URL. Example:
//
//     package google.profile;
//     message Person {
//       string first_name = 1;
//       string last_name = 2;
//     }
//
//     {
//       "@type": "type.googleapis.com/google.profile.Person",
//       "firstName": <string>,
//       "lastName": <string>
//     }
//
// If the embedded message type is well-known and has a custom JSON
// representation, that representation will be embedded adding a field
// `value` which holds the custom JSON in addition to the `@type`
// field. Example (for message [google.protobuf.Duration][]):
//
//     {
//       "@type": "type.googleapis.com/google.protobuf.Duration",
//       "value": "1.212s"
//     }
//
message Any {
  // A URL/resource name that uniquely identifies the type of the serialized
  // protocol buffer message. This string must contain at least
  // one "/" character. The last segment of the URL's path must represent
  // the fully qualified name of the type (as in
  // `path/google.protobuf.Duration`). The name should be in a canonical form
  // (e.g., leading "." is not accepted).
  //
  // In practice, teams usually precompile into the binary all types that they
  // expect it to use in the context of Any. However, for URLs which use the
  // scheme `http`, `https`, or no scheme, one can optionally set up a type
  // server that maps type URLs to message definitions as follows:
  //
  // * If no scheme is provided, `https` is assumed.
  // * An HTTP GET on the URL must yield a [google.protobuf.Type][]
  //   value in binary format, or produce an error.
  // * Applications are allowed to cache lookup results based on the
  //   URL, or have them precompiled into a binary to avoid any
  //   lookup. Therefore, binary compatibility needs to be preserved
  //   on changes to types. (Use versioned type names to manage
  //   breaking changes.)
  //
  // Note: this functionality is not currently available in the official
  // protobuf release, and it is not used for type URLs beginning with
  // type.googleapis.com.
  //
  // Schemes other than `http`, `https` (or the empty scheme) might be
  // used with implementation specific semantics.
  //
  string type_url = 1;

  // Must be a valid serialized protocol buffer of the above specified type.
  bytes value = 2;
}"#
        }
        CommonSchema::Api => {
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

import "google/protobuf/source_context.proto";
import "google/protobuf/type.proto";

option java_package = "com.google.protobuf";
option java_outer_classname = "ApiProto";
option java_multiple_files = true;
option objc_class_prefix = "GPB";
option csharp_namespace = "Google.Protobuf.WellKnownTypes";
option go_package = "google.golang.org/protobuf/types/known/apipb";

// Api is a light-weight descriptor for an API Interface.
//
// Interfaces are also described as "protocol buffer services" in some contexts,
// such as by the "service" keyword in a .proto file, but they are different
// from API Services, which represent a concrete implementation of an interface
// as opposed to simply a description of methods and bindings. They are also
// sometimes simply referred to as "APIs" in other contexts, such as the name of
// this message itself. See https://cloud.google.com/apis/design/glossary for
// detailed terminology.
message Api {
  // The fully qualified name of this interface, including package name
  // followed by the interface's simple name.
  string name = 1;

  // The methods of this interface, in unspecified order.
  repeated Method methods = 2;

  // Any metadata attached to the interface.
  repeated Option options = 3;

  // A version string for this interface. If specified, must have the form
  // `major-version.minor-version`, as in `1.10`. If the minor version is
  // omitted, it defaults to zero. If the entire version field is empty, the
  // major version is derived from the package name, as outlined below. If the
  // field is not empty, the version in the package name will be verified to be
  // consistent with what is provided here.
  //
  // The versioning schema uses [semantic
  // versioning](http://semver.org) where the major version number
  // indicates a breaking change and the minor version an additive,
  // non-breaking change. Both version numbers are signals to users
  // what to expect from different versions, and should be carefully
  // chosen based on the product plan.
  //
  // The major version is also reflected in the package name of the
  // interface, which must end in `v<major-version>`, as in
  // `google.feature.v1`. For major versions 0 and 1, the suffix can
  // be omitted. Zero major versions must only be used for
  // experimental, non-GA interfaces.
  //
  string version = 4;

  // Source context for the protocol buffer service represented by this
  // message.
  SourceContext source_context = 5;

  // Included interfaces. See [Mixin][].
  repeated Mixin mixins = 6;

  // The source syntax of the service.
  Syntax syntax = 7;
}

// Method represents a method of an API interface.
message Method {
  // The simple name of this method.
  string name = 1;

  // A URL of the input message type.
  string request_type_url = 2;

  // If true, the request is streamed.
  bool request_streaming = 3;

  // The URL of the output message type.
  string response_type_url = 4;

  // If true, the response is streamed.
  bool response_streaming = 5;

  // Any metadata attached to the method.
  repeated Option options = 6;

  // The source syntax of this method.
  Syntax syntax = 7;
}

// Declares an API Interface to be included in this interface. The including
// interface must redeclare all the methods from the included interface, but
// documentation and options are inherited as follows:
//
// - If after comment and whitespace stripping, the documentation
//   string of the redeclared method is empty, it will be inherited
//   from the original method.
//
// - Each annotation belonging to the service config (http,
//   visibility) which is not set in the redeclared method will be
//   inherited.
//
// - If an http annotation is inherited, the path pattern will be
//   modified as follows. Any version prefix will be replaced by the
//   version of the including interface plus the [root][] path if
//   specified.
//
// Example of a simple mixin:
//
//     package google.acl.v1;
//     service AccessControl {
//       // Get the underlying ACL object.
//       rpc GetAcl(GetAclRequest) returns (Acl) {
//         option (google.api.http).get = "/v1/{resource=**}:getAcl";
//       }
//     }
//
//     package google.storage.v2;
//     service Storage {
//       rpc GetAcl(GetAclRequest) returns (Acl);
//
//       // Get a data record.
//       rpc GetData(GetDataRequest) returns (Data) {
//         option (google.api.http).get = "/v2/{resource=**}";
//       }
//     }
//
// Example of a mixin configuration:
//
//     apis:
//     - name: google.storage.v2.Storage
//       mixins:
//       - name: google.acl.v1.AccessControl
//
// The mixin construct implies that all methods in `AccessControl` are
// also declared with same name and request/response types in
// `Storage`. A documentation generator or annotation processor will
// see the effective `Storage.GetAcl` method after inheriting
// documentation and annotations as follows:
//
//     service Storage {
//       // Get the underlying ACL object.
//       rpc GetAcl(GetAclRequest) returns (Acl) {
//         option (google.api.http).get = "/v2/{resource=**}:getAcl";
//       }
//       ...
//     }
//
// Note how the version in the path pattern changed from `v1` to `v2`.
//
// If the `root` field in the mixin is specified, it should be a
// relative path under which inherited HTTP paths are placed. Example:
//
//     apis:
//     - name: google.storage.v2.Storage
//       mixins:
//       - name: google.acl.v1.AccessControl
//         root: acls
//
// This implies the following inherited HTTP annotation:
//
//     service Storage {
//       // Get the underlying ACL object.
//       rpc GetAcl(GetAclRequest) returns (Acl) {
//         option (google.api.http).get = "/v2/acls/{resource=**}:getAcl";
//       }
//       ...
//     }
message Mixin {
  // The fully qualified name of the interface which is included.
  string name = 1;

  // If non-empty specifies a path under which inherited HTTP paths
  // are rooted.
  string root = 2;
}"#
        }
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
        CommonSchema::Descriptor => {
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

// Author: kenton@google.com (Kenton Varda)
//  Based on original Protocol Buffers design by
//  Sanjay Ghemawat, Jeff Dean, and others.
//
// The messages in this file describe the definitions found in .proto files.
// A valid .proto file can be translated directly to a FileDescriptorProto
// without any other information (e.g. without reading its imports).

syntax = "proto2";

package google.protobuf;

option go_package = "google.golang.org/protobuf/types/descriptorpb";
option java_package = "com.google.protobuf";
option java_outer_classname = "DescriptorProtos";
option csharp_namespace = "Google.Protobuf.Reflection";
option objc_class_prefix = "GPB";
option cc_enable_arenas = true;

// descriptor.proto must be optimized for speed because reflection-based
// algorithms don't work during bootstrapping.
option optimize_for = SPEED;

// The protocol compiler can output a FileDescriptorSet containing the .proto
// files it parses.
message FileDescriptorSet {
  repeated FileDescriptorProto file = 1;
}

// Describes a complete .proto file.
message FileDescriptorProto {
  optional string name = 1;     // file name, relative to root of source tree
  optional string package = 2;  // e.g. "foo", "foo.bar", etc.

  // Names of files imported by this file.
  repeated string dependency = 3;
  // Indexes of the public imported files in the dependency list above.
  repeated int32 public_dependency = 10;
  // Indexes of the weak imported files in the dependency list.
  // For Google-internal migration only. Do not use.
  repeated int32 weak_dependency = 11;

  // All top-level definitions in this file.
  repeated DescriptorProto message_type = 4;
  repeated EnumDescriptorProto enum_type = 5;
  repeated ServiceDescriptorProto service = 6;
  repeated FieldDescriptorProto extension = 7;

  optional FileOptions options = 8;

  // This field contains optional information about the original source code.
  // You may safely remove this entire field without harming runtime
  // functionality of the descriptors -- the information is needed only by
  // development tools.
  optional SourceCodeInfo source_code_info = 9;

  // The syntax of the proto file.
  // The supported values are "proto2", "proto3", and "editions".
  //
  // If `edition` is present, this value must be "editions".
  optional string syntax = 12;

  // The edition of the proto file, which is an opaque string.
  optional string edition = 13;
}

// Describes a message type.
message DescriptorProto {
  optional string name = 1;

  repeated FieldDescriptorProto field = 2;
  repeated FieldDescriptorProto extension = 6;

  repeated DescriptorProto nested_type = 3;
  repeated EnumDescriptorProto enum_type = 4;

  message ExtensionRange {
    optional int32 start = 1;  // Inclusive.
    optional int32 end = 2;    // Exclusive.

    optional ExtensionRangeOptions options = 3;
  }
  repeated ExtensionRange extension_range = 5;

  repeated OneofDescriptorProto oneof_decl = 8;

  optional MessageOptions options = 7;

  // Range of reserved tag numbers. Reserved tag numbers may not be used by
  // fields or extension ranges in the same message. Reserved ranges may
  // not overlap.
  message ReservedRange {
    optional int32 start = 1;  // Inclusive.
    optional int32 end = 2;    // Exclusive.
  }
  repeated ReservedRange reserved_range = 9;
  // Reserved field names, which may not be used by fields in the same message.
  // A given name may only be reserved once.
  repeated string reserved_name = 10;
}

message ExtensionRangeOptions {
  // The parser stores options it doesn't recognize here. See above.
  repeated UninterpretedOption uninterpreted_option = 999;

  // Clients can define custom options in extensions of this message. See above.
  extensions 1000 to max;
}

// Describes a field within a message.
message FieldDescriptorProto {
  enum Type {
    // 0 is reserved for errors.
    // Order is weird for historical reasons.
    TYPE_DOUBLE = 1;
    TYPE_FLOAT = 2;
    // Not ZigZag encoded.  Negative numbers take 10 bytes.  Use TYPE_SINT64 if
    // negative values are likely.
    TYPE_INT64 = 3;
    TYPE_UINT64 = 4;
    // Not ZigZag encoded.  Negative numbers take 10 bytes.  Use TYPE_SINT32 if
    // negative values are likely.
    TYPE_INT32 = 5;
    TYPE_FIXED64 = 6;
    TYPE_FIXED32 = 7;
    TYPE_BOOL = 8;
    TYPE_STRING = 9;
    // Tag-delimited aggregate.
    // Group type is deprecated and not supported in proto3. However, Proto3
    // implementations should still be able to parse the group wire format and
    // treat group fields as unknown fields.
    TYPE_GROUP = 10;
    TYPE_MESSAGE = 11;  // Length-delimited aggregate.

    // New in version 2.
    TYPE_BYTES = 12;
    TYPE_UINT32 = 13;
    TYPE_ENUM = 14;
    TYPE_SFIXED32 = 15;
    TYPE_SFIXED64 = 16;
    TYPE_SINT32 = 17;  // Uses ZigZag encoding.
    TYPE_SINT64 = 18;  // Uses ZigZag encoding.
  }

  enum Label {
    // 0 is reserved for errors
    LABEL_OPTIONAL = 1;
    LABEL_REQUIRED = 2;
    LABEL_REPEATED = 3;
  }

  optional string name = 1;
  optional int32 number = 3;
  optional Label label = 4;

  // If type_name is set, this need not be set.  If both this and type_name
  // are set, this must be one of TYPE_ENUM, TYPE_MESSAGE or TYPE_GROUP.
  optional Type type = 5;

  // For message and enum types, this is the name of the type.  If the name
  // starts with a '.', it is fully-qualified.  Otherwise, C++-like scoping
  // rules are used to find the type (i.e. first the nested types within this
  // message are searched, then within the parent, on up to the root
  // namespace).
  optional string type_name = 6;

  // For extensions, this is the name of the type being extended.  It is
  // resolved in the same manner as type_name.
  optional string extendee = 2;

  // For numeric types, contains the original text representation of the value.
  // For booleans, "true" or "false".
  // For strings, contains the default text contents (not escaped in any way).
  // For bytes, contains the C escaped value.  All bytes >= 128 are escaped.
  optional string default_value = 7;

  // If set, gives the index of a oneof in the containing type's oneof_decl
  // list.  This field is a member of that oneof.
  optional int32 oneof_index = 9;

  // JSON name of this field. The value is set by protocol compiler. If the
  // user has set a "json_name" option on this field, that option's value
  // will be used. Otherwise, it's deduced from the field's name by converting
  // it to camelCase.
  optional string json_name = 10;

  optional FieldOptions options = 8;

  // If true, this is a proto3 "optional". When a proto3 field is optional, it
  // tracks presence regardless of field type.
  //
  // When proto3_optional is true, this field must be belong to a oneof to
  // signal to old proto3 clients that presence is tracked for this field. This
  // oneof is known as a "synthetic" oneof, and this field must be its sole
  // member (each proto3 optional field gets its own synthetic oneof). Synthetic
  // oneofs exist in the descriptor only, and do not generate any API. Synthetic
  // oneofs must be ordered after all "real" oneofs.
  //
  // For message fields, proto3_optional doesn't create any semantic change,
  // since non-repeated message fields always track presence. However it still
  // indicates the semantic detail of whether the user wrote "optional" or not.
  // This can be useful for round-tripping the .proto file. For consistency we
  // give message fields a synthetic oneof also, even though it is not required
  // to track presence. This is especially important because the parser can't
  // tell if a field is a message or an enum, so it must always create a
  // synthetic oneof.
  //
  // Proto2 optional fields do not set this flag, because they already indicate
  // optional with `LABEL_OPTIONAL`.
  optional bool proto3_optional = 17;
}

// Describes a oneof.
message OneofDescriptorProto {
  optional string name = 1;
  optional OneofOptions options = 2;
}

// Describes an enum type.
message EnumDescriptorProto {
  optional string name = 1;

  repeated EnumValueDescriptorProto value = 2;

  optional EnumOptions options = 3;

  // Range of reserved numeric values. Reserved values may not be used by
  // entries in the same enum. Reserved ranges may not overlap.
  //
  // Note that this is distinct from DescriptorProto.ReservedRange in that it
  // is inclusive such that it can appropriately represent the entire int32
  // domain.
  message EnumReservedRange {
    optional int32 start = 1;  // Inclusive.
    optional int32 end = 2;    // Inclusive.
  }

  // Range of reserved numeric values. Reserved numeric values may not be used
  // by enum values in the same enum declaration. Reserved ranges may not
  // overlap.
  repeated EnumReservedRange reserved_range = 4;

  // Reserved enum value names, which may not be reused. A given name may only
  // be reserved once.
  repeated string reserved_name = 5;
}

// Describes a value within an enum.
message EnumValueDescriptorProto {
  optional string name = 1;
  optional int32 number = 2;

  optional EnumValueOptions options = 3;
}

// Describes a service.
message ServiceDescriptorProto {
  optional string name = 1;
  repeated MethodDescriptorProto method = 2;

  optional ServiceOptions options = 3;
}

// Describes a method of a service.
message MethodDescriptorProto {
  optional string name = 1;

  // Input and output type names.  These are resolved in the same way as
  // FieldDescriptorProto.type_name, but must refer to a message type.
  optional string input_type = 2;
  optional string output_type = 3;

  optional MethodOptions options = 4;

  // Identifies if client streams multiple client messages
  optional bool client_streaming = 5 [default = false];
  // Identifies if server streams multiple server messages
  optional bool server_streaming = 6 [default = false];
}

// ===================================================================
// Options

// Each of the definitions above may have "options" attached.  These are
// just annotations which may cause code to be generated slightly differently
// or may contain hints for code that manipulates protocol messages.
//
// Clients may define custom options as extensions of the *Options messages.
// These extensions may not yet be known at parsing time, so the parser cannot
// store the values in them.  Instead it stores them in a field in the *Options
// message called uninterpreted_option. This field must have the same name
// across all *Options messages. We then use this field to populate the
// extensions when we build a descriptor, at which point all protos have been
// parsed and so all extensions are known.
//
// Extension numbers for custom options may be chosen as follows:
// * For options which will only be used within a single application or
//   organization, or for experimental options, use field numbers 50000
//   through 99999.  It is up to you to ensure that you do not use the
//   same number for multiple options.
// * For options which will be published and used publicly by multiple
//   independent entities, e-mail protobuf-global-extension-registry@google.com
//   to reserve extension numbers. Simply provide your project name (e.g.
//   Objective-C plugin) and your project website (if available) -- there's no
//   need to explain how you intend to use them. Usually you only need one
//   extension number. You can declare multiple options with only one extension
//   number by putting them in a sub-message. See the Custom Options section of
//   the docs for examples:
//   https://developers.google.com/protocol-buffers/docs/proto#options
//   If this turns out to be popular, a web service will be set up
//   to automatically assign option numbers.

message FileOptions {

  // Sets the Java package where classes generated from this .proto will be
  // placed.  By default, the proto package is used, but this is often
  // inappropriate because proto packages do not normally start with backwards
  // domain names.
  optional string java_package = 1;

  // Controls the name of the wrapper Java class generated for the .proto file.
  // That class will always contain the .proto file's getDescriptor() method as
  // well as any top-level extensions defined in the .proto file.
  // If java_multiple_files is disabled, then all the other classes from the
  // .proto file will be nested inside the single wrapper outer class.
  optional string java_outer_classname = 8;

  // If enabled, then the Java code generator will generate a separate .java
  // file for each top-level message, enum, and service defined in the .proto
  // file.  Thus, these types will *not* be nested inside the wrapper class
  // named by java_outer_classname.  However, the wrapper class will still be
  // generated to contain the file's getDescriptor() method as well as any
  // top-level extensions defined in the file.
  optional bool java_multiple_files = 10 [default = false];

  // This option does nothing.
  optional bool java_generate_equals_and_hash = 20 [deprecated=true];

  // If set true, then the Java2 code generator will generate code that
  // throws an exception whenever an attempt is made to assign a non-UTF-8
  // byte sequence to a string field.
  // Message reflection will do the same.
  // However, an extension field still accepts non-UTF-8 byte sequences.
  // This option has no effect on when used with the lite runtime.
  optional bool java_string_check_utf8 = 27 [default = false];

  // Generated classes can be optimized for speed or code size.
  enum OptimizeMode {
    SPEED = 1;         // Generate complete code for parsing, serialization,
                       // etc.
    CODE_SIZE = 2;     // Use ReflectionOps to implement these methods.
    LITE_RUNTIME = 3;  // Generate code using MessageLite and the lite runtime.
  }
  optional OptimizeMode optimize_for = 9 [default = SPEED];

  // Sets the Go package where structs generated from this .proto will be
  // placed. If omitted, the Go package will be derived from the following:
  //   - The basename of the package import path, if provided.
  //   - Otherwise, the package statement in the .proto file, if present.
  //   - Otherwise, the basename of the .proto file, without extension.
  optional string go_package = 11;

  // Should generic services be generated in each language?  "Generic" services
  // are not specific to any particular RPC system.  They are generated by the
  // main code generators in each language (without additional plugins).
  // Generic services were the only kind of service generation supported by
  // early versions of google.protobuf.
  //
  // Generic services are now considered deprecated in favor of using plugins
  // that generate code specific to your particular RPC system.  Therefore,
  // these default to false.  Old code which depends on generic services should
  // explicitly set them to true.
  optional bool cc_generic_services = 16 [default = false];
  optional bool java_generic_services = 17 [default = false];
  optional bool py_generic_services = 18 [default = false];
  optional bool php_generic_services = 42 [default = false];

  // Is this file deprecated?
  // Depending on the target platform, this can emit Deprecated annotations
  // for everything in the file, or it will be completely ignored; in the very
  // least, this is a formalization for deprecating files.
  optional bool deprecated = 23 [default = false];

  // Enables the use of arenas for the proto messages in this file. This applies
  // only to generated classes for C++.
  optional bool cc_enable_arenas = 31 [default = true];

  // Sets the objective c class prefix which is prepended to all objective c
  // generated classes from this .proto. There is no default.
  optional string objc_class_prefix = 36;

  // Namespace for generated classes; defaults to the package.
  optional string csharp_namespace = 37;

  // By default Swift generators will take the proto package and CamelCase it
  // replacing '.' with underscore and use that to prefix the types/symbols
  // defined. When this options is provided, they will use this value instead
  // to prefix the types/symbols defined.
  optional string swift_prefix = 39;

  // Sets the php class prefix which is prepended to all php generated classes
  // from this .proto. Default is empty.
  optional string php_class_prefix = 40;

  // Use this option to change the namespace of php generated classes. Default
  // is empty. When this option is empty, the package name will be used for
  // determining the namespace.
  optional string php_namespace = 41;

  // Use this option to change the namespace of php generated metadata classes.
  // Default is empty. When this option is empty, the proto file name will be
  // used for determining the namespace.
  optional string php_metadata_namespace = 44;

  // Use this option to change the package of ruby generated classes. Default
  // is empty. When this option is not set, the package name will be used for
  // determining the ruby package.
  optional string ruby_package = 45;

  // The parser stores options it doesn't recognize here.
  // See the documentation for the "Options" section above.
  repeated UninterpretedOption uninterpreted_option = 999;

  // Clients can define custom options in extensions of this message.
  // See the documentation for the "Options" section above.
  extensions 1000 to max;

  reserved 38;
}

message MessageOptions {
  // Set true to use the old proto1 MessageSet wire format for extensions.
  // This is provided for backwards-compatibility with the MessageSet wire
  // format.  You should not use this for any other reason:  It's less
  // efficient, has fewer features, and is more complicated.
  //
  // The message must be defined exactly as follows:
  //   message Foo {
  //     option message_set_wire_format = true;
  //     extensions 4 to max;
  //   }
  // Note that the message cannot have any defined fields; MessageSets only
  // have extensions.
  //
  // All extensions of your type must be singular messages; e.g. they cannot
  // be int32s, enums, or repeated messages.
  //
  // Because this is an option, the above two restrictions are not enforced by
  // the protocol compiler.
  optional bool message_set_wire_format = 1 [default = false];

  // Disables the generation of the standard "descriptor()" accessor, which can
  // conflict with a field of the same name.  This is meant to make migration
  // from proto1 easier; new code should avoid fields named "descriptor".
  optional bool no_standard_descriptor_accessor = 2 [default = false];

  // Is this message deprecated?
  // Depending on the target platform, this can emit Deprecated annotations
  // for the message, or it will be completely ignored; in the very least,
  // this is a formalization for deprecating messages.
  optional bool deprecated = 3 [default = false];

  reserved 4, 5, 6;

  // NOTE: Do not set the option in .proto files. Always use the maps syntax
  // instead. The option should only be implicitly set by the proto compiler
  // parser.
  //
  // Whether the message is an automatically generated map entry type for the
  // maps field.
  //
  // For maps fields:
  //     map<KeyType, ValueType> map_field = 1;
  // The parsed descriptor looks like:
  //     message MapFieldEntry {
  //         option map_entry = true;
  //         optional KeyType key = 1;
  //         optional ValueType value = 2;
  //     }
  //     repeated MapFieldEntry map_field = 1;
  //
  // Implementations may choose not to generate the map_entry=true message, but
  // use a native map in the target language to hold the keys and values.
  // The reflection APIs in such implementations still need to work as
  // if the field is a repeated message field.
  optional bool map_entry = 7;

  reserved 8;  // javalite_serializable
  reserved 9;  // javanano_as_lite

  // The parser stores options it doesn't recognize here. See above.
  repeated UninterpretedOption uninterpreted_option = 999;

  // Clients can define custom options in extensions of this message. See above.
  extensions 1000 to max;
}

message FieldOptions {
  // The ctype option instructs the C++ code generator to use a different
  // representation of the field than it normally would.  See the specific
  // options below.  This option is not yet implemented in the open source
  // release -- sorry, we'll try to include it in a future version!
  optional CType ctype = 1 [default = STRING];
  enum CType {
    // Default mode.
    STRING = 0;

    CORD = 1;

    STRING_PIECE = 2;
  }
  // The packed option can be enabled for repeated primitive fields to enable
  // a more efficient representation on the wire. Rather than repeatedly
  // writing the tag and type for each element, the entire array is encoded as
  // a single length-delimited blob. In proto3, only explicit setting it to
  // false will avoid using packed encoding.
  optional bool packed = 2;

  // The jstype option determines the JavaScript type used for values of the
  // field.  The option is permitted only for 64 bit integral and fixed types
  // (int64, uint64, sint64, fixed64, sfixed64).  A field with jstype JS_STRING
  // is represented as JavaScript string, which avoids loss of precision that
  // can happen when a large value is converted to a floating point JavaScript.
  // Specifying JS_NUMBER for the jstype causes the generated JavaScript code to
  // use the JavaScript "number" type.  The behavior of the default option
  // JS_NORMAL is implementation dependent.
  //
  // This option is an enum to permit additional types to be added, e.g.
  // goog.math.Integer.
  optional JSType jstype = 6 [default = JS_NORMAL];
  enum JSType {
    // Use the default type.
    JS_NORMAL = 0;

    // Use JavaScript strings.
    JS_STRING = 1;

    // Use JavaScript numbers.
    JS_NUMBER = 2;
  }

  // Should this field be parsed lazily?  Lazy applies only to message-type
  // fields.  It means that when the outer message is initially parsed, the
  // inner message's contents will not be parsed but instead stored in encoded
  // form.  The inner message will actually be parsed when it is first accessed.
  //
  // This is only a hint.  Implementations are free to choose whether to use
  // eager or lazy parsing regardless of the value of this option.  However,
  // setting this option true suggests that the protocol author believes that
  // using lazy parsing on this field is worth the additional bookkeeping
  // overhead typically needed to implement it.
  //
  // This option does not affect the public interface of any generated code;
  // all method signatures remain the same.  Furthermore, thread-safety of the
  // interface is not affected by this option; const methods remain safe to
  // call from multiple threads concurrently, while non-const methods continue
  // to require exclusive access.
  //
  // Note that implementations may choose not to check required fields within
  // a lazy sub-message.  That is, calling IsInitialized() on the outer message
  // may return true even if the inner message has missing required fields.
  // This is necessary because otherwise the inner message would have to be
  // parsed in order to perform the check, defeating the purpose of lazy
  // parsing.  An implementation which chooses not to check required fields
  // must be consistent about it.  That is, for any particular sub-message, the
  // implementation must either *always* check its required fields, or *never*
  // check its required fields, regardless of whether or not the message has
  // been parsed.
  //
  // As of May 2022, lazy verifies the contents of the byte stream during
  // parsing.  An invalid byte stream will cause the overall parsing to fail.
  optional bool lazy = 5 [default = false];

  // unverified_lazy does no correctness checks on the byte stream. This should
  // only be used where lazy with verification is prohibitive for performance
  // reasons.
  optional bool unverified_lazy = 15 [default = false];

  // Is this field deprecated?
  // Depending on the target platform, this can emit Deprecated annotations
  // for accessors, or it will be completely ignored; in the very least, this
  // is a formalization for deprecating fields.
  optional bool deprecated = 3 [default = false];

  // For Google-internal migration only. Do not use.
  optional bool weak = 10 [default = false];

  // The parser stores options it doesn't recognize here. See above.
  repeated UninterpretedOption uninterpreted_option = 999;

  // Clients can define custom options in extensions of this message. See above.
  extensions 1000 to max;

  reserved 4;  // removed jtype
}

message OneofOptions {
  // The parser stores options it doesn't recognize here. See above.
  repeated UninterpretedOption uninterpreted_option = 999;

  // Clients can define custom options in extensions of this message. See above.
  extensions 1000 to max;
}

message EnumOptions {

  // Set this option to true to allow mapping different tag names to the same
  // value.
  optional bool allow_alias = 2;

  // Is this enum deprecated?
  // Depending on the target platform, this can emit Deprecated annotations
  // for the enum, or it will be completely ignored; in the very least, this
  // is a formalization for deprecating enums.
  optional bool deprecated = 3 [default = false];

  reserved 5;  // javanano_as_lite

  // The parser stores options it doesn't recognize here. See above.
  repeated UninterpretedOption uninterpreted_option = 999;

  // Clients can define custom options in extensions of this message. See above.
  extensions 1000 to max;
}

message EnumValueOptions {
  // Is this enum value deprecated?
  // Depending on the target platform, this can emit Deprecated annotations
  // for the enum value, or it will be completely ignored; in the very least,
  // this is a formalization for deprecating enum values.
  optional bool deprecated = 1 [default = false];

  // The parser stores options it doesn't recognize here. See above.
  repeated UninterpretedOption uninterpreted_option = 999;

  // Clients can define custom options in extensions of this message. See above.
  extensions 1000 to max;
}

message ServiceOptions {

  // Note:  Field numbers 1 through 32 are reserved for Google's internal RPC
  //   framework.  We apologize for hoarding these numbers to ourselves, but
  //   we were already using them long before we decided to release Protocol
  //   Buffers.

  // Is this service deprecated?
  // Depending on the target platform, this can emit Deprecated annotations
  // for the service, or it will be completely ignored; in the very least,
  // this is a formalization for deprecating services.
  optional bool deprecated = 33 [default = false];

  // The parser stores options it doesn't recognize here. See above.
  repeated UninterpretedOption uninterpreted_option = 999;

  // Clients can define custom options in extensions of this message. See above.
  extensions 1000 to max;
}

message MethodOptions {

  // Note:  Field numbers 1 through 32 are reserved for Google's internal RPC
  //   framework.  We apologize for hoarding these numbers to ourselves, but
  //   we were already using them long before we decided to release Protocol
  //   Buffers.

  // Is this method deprecated?
  // Depending on the target platform, this can emit Deprecated annotations
  // for the method, or it will be completely ignored; in the very least,
  // this is a formalization for deprecating methods.
  optional bool deprecated = 33 [default = false];

  // Is this method side-effect-free (or safe in HTTP parlance), or idempotent,
  // or neither? HTTP based RPC implementation may choose GET verb for safe
  // methods, and PUT verb for idempotent methods instead of the default POST.
  enum IdempotencyLevel {
    IDEMPOTENCY_UNKNOWN = 0;
    NO_SIDE_EFFECTS = 1;  // implies idempotent
    IDEMPOTENT = 2;       // idempotent, but may have side effects
  }
  optional IdempotencyLevel idempotency_level = 34
      [default = IDEMPOTENCY_UNKNOWN];

  // The parser stores options it doesn't recognize here. See above.
  repeated UninterpretedOption uninterpreted_option = 999;

  // Clients can define custom options in extensions of this message. See above.
  extensions 1000 to max;
}

// A message representing a option the parser does not recognize. This only
// appears in options protos created by the compiler::Parser class.
// DescriptorPool resolves these when building Descriptor objects. Therefore,
// options protos in descriptor objects (e.g. returned by Descriptor::options(),
// or produced by Descriptor::CopyTo()) will never have UninterpretedOptions
// in them.
message UninterpretedOption {
  // The name of the uninterpreted option.  Each string represents a segment in
  // a dot-separated name.  is_extension is true iff a segment represents an
  // extension (denoted with parentheses in options specs in .proto files).
  // E.g.,{ ["foo", false], ["bar.baz", true], ["moo", false] } represents
  // "foo.(bar.baz).moo".
  message NamePart {
    required string name_part = 1;
    required bool is_extension = 2;
  }
  repeated NamePart name = 2;

  // The value of the uninterpreted option, in whatever type the tokenizer
  // identified it as during parsing. Exactly one of these should be set.
  optional string identifier_value = 3;
  optional uint64 positive_int_value = 4;
  optional int64 negative_int_value = 5;
  optional double double_value = 6;
  optional bytes string_value = 7;
  optional string aggregate_value = 8;
}

// ===================================================================
// Optional source code info

// Encapsulates information about the original source file from which a
// FileDescriptorProto was generated.
message SourceCodeInfo {
  // A Location identifies a piece of source code in a .proto file which
  // corresponds to a particular definition.  This information is intended
  // to be useful to IDEs, code indexers, documentation generators, and similar
  // tools.
  //
  // For example, say we have a file like:
  //   message Foo {
  //     optional string foo = 1;
  //   }
  // Let's look at just the field definition:
  //   optional string foo = 1;
  //   ^       ^^     ^^  ^  ^^^
  //   a       bc     de  f  ghi
  // We have the following locations:
  //   span   path               represents
  //   [a,i)  [ 4, 0, 2, 0 ]     The whole field definition.
  //   [a,b)  [ 4, 0, 2, 0, 4 ]  The label (optional).
  //   [c,d)  [ 4, 0, 2, 0, 5 ]  The type (string).
  //   [e,f)  [ 4, 0, 2, 0, 1 ]  The name (foo).
  //   [g,h)  [ 4, 0, 2, 0, 3 ]  The number (1).
  //
  // Notes:
  // - A location may refer to a repeated field itself (i.e. not to any
  //   particular index within it).  This is used whenever a set of elements are
  //   logically enclosed in a single code segment.  For example, an entire
  //   extend block (possibly containing multiple extension definitions) will
  //   have an outer location whose path refers to the "extensions" repeated
  //   field without an index.
  // - Multiple locations may have the same path.  This happens when a single
  //   logical declaration is spread out across multiple places.  The most
  //   obvious example is the "extend" block again -- there may be multiple
  //   extend blocks in the same scope, each of which will have the same path.
  // - A location's span is not always a subset of its parent's span.  For
  //   example, the "extendee" of an extension declaration appears at the
  //   beginning of the "extend" block and is shared by all extensions within
  //   the block.
  // - Just because a location's span is a subset of some other location's span
  //   does not mean that it is a descendant.  For example, a "group" defines
  //   both a type and a field in a single declaration.  Thus, the locations
  //   corresponding to the type and field and their components will overlap.
  // - Code which tries to interpret locations should probably be designed to
  //   ignore those that it doesn't understand, as more types of locations could
  //   be recorded in the future.
  repeated Location location = 1;
  message Location {
    // Identifies which part of the FileDescriptorProto was defined at this
    // location.
    //
    // Each element is a field number or an index.  They form a path from
    // the root FileDescriptorProto to the place where the definition occurs.
    // For example, this path:
    //   [ 4, 3, 2, 7, 1 ]
    // refers to:
    //   file.message_type(3)  // 4, 3
    //       .field(7)         // 2, 7
    //       .name()           // 1
    // This is because FileDescriptorProto.message_type has field number 4:
    //   repeated DescriptorProto message_type = 4;
    // and DescriptorProto.field has field number 2:
    //   repeated FieldDescriptorProto field = 2;
    // and FieldDescriptorProto.name has field number 1:
    //   optional string name = 1;
    //
    // Thus, the above path gives the location of a field name.  If we removed
    // the last element:
    //   [ 4, 3, 2, 7 ]
    // this path refers to the whole field declaration (from the beginning
    // of the label to the terminating semicolon).
    repeated int32 path = 1 [packed = true];

    // Always has exactly three or four elements: start line, start column,
    // end line (optional, otherwise assumed same as start line), end column.
    // These are packed into a single field for efficiency.  Note that line
    // and column numbers are zero-based -- typically you will want to add
    // 1 to each before displaying to a user.
    repeated int32 span = 2 [packed = true];

    // If this SourceCodeInfo represents a complete declaration, these are any
    // comments appearing before and after the declaration which appear to be
    // attached to the declaration.
    //
    // A series of line comments appearing on consecutive lines, with no other
    // tokens appearing on those lines, will be treated as a single comment.
    //
    // leading_detached_comments will keep paragraphs of comments that appear
    // before (but not connected to) the current element. Each paragraph,
    // separated by empty lines, will be one comment element in the repeated
    // field.
    //
    // Only the comment content is provided; comment markers (e.g. //) are
    // stripped out.  For block comments, leading whitespace and an asterisk
    // will be stripped from the beginning of each line other than the first.
    // Newlines are included in the output.
    //
    // Examples:
    //
    //   optional int32 foo = 1;  // Comment attached to foo.
    //   // Comment attached to bar.
    //   optional int32 bar = 2;
    //
    //   optional string baz = 3;
    //   // Comment attached to baz.
    //   // Another line attached to baz.
    //
    //   // Comment attached to moo.
    //   //
    //   // Another line attached to moo.
    //   optional double moo = 4;
    //
    //   // Detached comment for corge. This is not leading or trailing comments
    //   // to moo or corge because there are blank lines separating it from
    //   // both.
    //
    //   // Detached comment for corge paragraph 2.
    //
    //   optional string corge = 5;
    //   /* Block comment attached
    //    * to corge.  Leading asterisks
    //    * will be removed. */
    //   /* Block comment attached to
    //    * grault. */
    //   optional int32 grault = 6;
    //
    //   // ignored detached comments.
    optional string leading_comments = 3;
    optional string trailing_comments = 4;
    repeated string leading_detached_comments = 6;
  }
}

// Describes the relationship between generated code and its original source
// file. A GeneratedCodeInfo message is associated with only one generated
// source file, but may contain references to different source .proto files.
message GeneratedCodeInfo {
  // An Annotation connects some span of text in generated code to an element
  // of its generating .proto file.
  repeated Annotation annotation = 1;
  message Annotation {
    // Identifies the element in the original source .proto file. This field
    // is formatted the same as SourceCodeInfo.Location.path.
    repeated int32 path = 1 [packed = true];

    // Identifies the filesystem path to the original source .proto.
    optional string source_file = 2;

    // Identifies the starting offset in bytes in the generated code
    // that relates to the identified object.
    optional int32 begin = 3;

    // Identifies the ending offset in bytes in the generated code that
    // relates to the identified object. The end offset should be one past
    // the last relevant byte (so the length of the text = end - begin).
    optional int32 end = 4;

    // Represents the identified object's effect on the element in the original
    // .proto file.
    enum Semantic {
      // There is no effect or the effect is indescribable.
      NONE = 0;
      // The element is set or otherwise mutated.
      SET = 1;
      // An alias to the element is returned.
      ALIAS = 2;
    }
    optional Semantic semantic = 5;
  }
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
        CommonSchema::Empty => {
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

option go_package = "google.golang.org/protobuf/types/known/emptypb";
option java_package = "com.google.protobuf";
option java_outer_classname = "EmptyProto";
option java_multiple_files = true;
option objc_class_prefix = "GPB";
option csharp_namespace = "Google.Protobuf.WellKnownTypes";
option cc_enable_arenas = true;

// A generic empty message that you can re-use to avoid defining duplicated
// empty messages in your APIs. A typical example is to use it as the request
// or the response type of an API method. For instance:
//
//     service Foo {
//       rpc Bar(google.protobuf.Empty) returns (google.protobuf.Empty);
//     }
//
message Empty {}"#
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
        CommonSchema::FieldMask => {
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

option java_package = "com.google.protobuf";
option java_outer_classname = "FieldMaskProto";
option java_multiple_files = true;
option objc_class_prefix = "GPB";
option csharp_namespace = "Google.Protobuf.WellKnownTypes";
option go_package = "google.golang.org/protobuf/types/known/fieldmaskpb";
option cc_enable_arenas = true;

// `FieldMask` represents a set of symbolic field paths, for example:
//
//     paths: "f.a"
//     paths: "f.b.d"
//
// Here `f` represents a field in some root message, `a` and `b`
// fields in the message found in `f`, and `d` a field found in the
// message in `f.b`.
//
// Field masks are used to specify a subset of fields that should be
// returned by a get operation or modified by an update operation.
// Field masks also have a custom JSON encoding (see below).
//
// # Field Masks in Projections
//
// When used in the context of a projection, a response message or
// sub-message is filtered by the API to only contain those fields as
// specified in the mask. For example, if the mask in the previous
// example is applied to a response message as follows:
//
//     f {
//       a : 22
//       b {
//         d : 1
//         x : 2
//       }
//       y : 13
//     }
//     z: 8
//
// The result will not contain specific values for fields x,y and z
// (their value will be set to the default, and omitted in proto text
// output):
//
//
//     f {
//       a : 22
//       b {
//         d : 1
//       }
//     }
//
// A repeated field is not allowed except at the last position of a
// paths string.
//
// If a FieldMask object is not present in a get operation, the
// operation applies to all fields (as if a FieldMask of all fields
// had been specified).
//
// Note that a field mask does not necessarily apply to the
// top-level response message. In case of a REST get operation, the
// field mask applies directly to the response, but in case of a REST
// list operation, the mask instead applies to each individual message
// in the returned resource list. In case of a REST custom method,
// other definitions may be used. Where the mask applies will be
// clearly documented together with its declaration in the API.  In
// any case, the effect on the returned resource/resources is required
// behavior for APIs.
//
// # Field Masks in Update Operations
//
// A field mask in update operations specifies which fields of the
// targeted resource are going to be updated. The API is required
// to only change the values of the fields as specified in the mask
// and leave the others untouched. If a resource is passed in to
// describe the updated values, the API ignores the values of all
// fields not covered by the mask.
//
// If a repeated field is specified for an update operation, new values will
// be appended to the existing repeated field in the target resource. Note that
// a repeated field is only allowed in the last position of a `paths` string.
//
// If a sub-message is specified in the last position of the field mask for an
// update operation, then new value will be merged into the existing sub-message
// in the target resource.
//
// For example, given the target message:
//
//     f {
//       b {
//         d: 1
//         x: 2
//       }
//       c: [1]
//     }
//
// And an update message:
//
//     f {
//       b {
//         d: 10
//       }
//       c: [2]
//     }
//
// then if the field mask is:
//
//  paths: ["f.b", "f.c"]
//
// then the result will be:
//
//     f {
//       b {
//         d: 10
//         x: 2
//       }
//       c: [1, 2]
//     }
//
// An implementation may provide options to override this default behavior for
// repeated and message fields.
//
// In order to reset a field's value to the default, the field must
// be in the mask and set to the default value in the provided resource.
// Hence, in order to reset all fields of a resource, provide a default
// instance of the resource and set all fields in the mask, or do
// not provide a mask as described below.
//
// If a field mask is not present on update, the operation applies to
// all fields (as if a field mask of all fields has been specified).
// Note that in the presence of schema evolution, this may mean that
// fields the client does not know and has therefore not filled into
// the request will be reset to their default. If this is unwanted
// behavior, a specific service may require a client to always specify
// a field mask, producing an error if not.
//
// As with get operations, the location of the resource which
// describes the updated values in the request message depends on the
// operation kind. In any case, the effect of the field mask is
// required to be honored by the API.
//
// ## Considerations for HTTP REST
//
// The HTTP kind of an update operation which uses a field mask must
// be set to PATCH instead of PUT in order to satisfy HTTP semantics
// (PUT must only be used for full updates).
//
// # JSON Encoding of Field Masks
//
// In JSON, a field mask is encoded as a single string where paths are
// separated by a comma. Fields name in each path are converted
// to/from lower-camel naming conventions.
//
// As an example, consider the following message declarations:
//
//     message Profile {
//       User user = 1;
//       Photo photo = 2;
//     }
//     message User {
//       string display_name = 1;
//       string address = 2;
//     }
//
// In proto a field mask for `Profile` may look as such:
//
//     mask {
//       paths: "user.display_name"
//       paths: "photo"
//     }
//
// In JSON, the same mask is represented as below:
//
//     {
//       mask: "user.displayName,photo"
//     }
//
// # Field Masks and Oneof Fields
//
// Field masks treat fields in oneofs just as regular fields. Consider the
// following message:
//
//     message SampleMessage {
//       oneof test_oneof {
//         string name = 4;
//         SubMessage sub_message = 9;
//       }
//     }
//
// The field mask can be:
//
//     mask {
//       paths: "name"
//     }
//
// Or:
//
//     mask {
//       paths: "sub_message"
//     }
//
// Note that oneof type names ("test_oneof" in this case) cannot be used in
// paths.
//
// ## Field Mask Verification
//
// The implementation of any API method which has a FieldMask type field in the
// request should verify the included field paths, and return an
// `INVALID_ARGUMENT` error if any path is unmappable.
message FieldMask {
  // The set of field mask paths.
  repeated string paths = 1;
}"#
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
        CommonSchema::SourceContext => {
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

option java_package = "com.google.protobuf";
option java_outer_classname = "SourceContextProto";
option java_multiple_files = true;
option objc_class_prefix = "GPB";
option csharp_namespace = "Google.Protobuf.WellKnownTypes";
option go_package = "google.golang.org/protobuf/types/known/sourcecontextpb";

// `SourceContext` represents information about the source of a
// protobuf element, like the file in which it is defined.
message SourceContext {
  // The path-qualified name of the .proto file that contained the associated
  // protobuf element.  For example: `"google/protobuf/source_context.proto"`.
  string file_name = 1;
}"#
        }
        CommonSchema::Struct => {
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

option cc_enable_arenas = true;
option go_package = "google.golang.org/protobuf/types/known/structpb";
option java_package = "com.google.protobuf";
option java_outer_classname = "StructProto";
option java_multiple_files = true;
option objc_class_prefix = "GPB";
option csharp_namespace = "Google.Protobuf.WellKnownTypes";

// `Struct` represents a structured data value, consisting of fields
// which map to dynamically typed values. In some languages, `Struct`
// might be supported by a native representation. For example, in
// scripting languages like JS a struct is represented as an
// object. The details of that representation are described together
// with the proto support for the language.
//
// The JSON representation for `Struct` is JSON object.
message Struct {
  // Unordered map of dynamically typed values.
  map<string, Value> fields = 1;
}

// `Value` represents a dynamically typed value which can be either
// null, a number, a string, a boolean, a recursive struct value, or a
// list of values. A producer of value is expected to set one of these
// variants. Absence of any variant indicates an error.
//
// The JSON representation for `Value` is JSON value.
message Value {
  // The kind of value.
  oneof kind {
    // Represents a null value.
    NullValue null_value = 1;
    // Represents a double value.
    double number_value = 2;
    // Represents a string value.
    string string_value = 3;
    // Represents a boolean value.
    bool bool_value = 4;
    // Represents a structured value.
    Struct struct_value = 5;
    // Represents a repeated `Value`.
    ListValue list_value = 6;
  }
}

// `NullValue` is a singleton enumeration to represent the null value for the
// `Value` type union.
//
//  The JSON representation for `NullValue` is JSON `null`.
enum NullValue {
  // Null value.
  NULL_VALUE = 0;
}

// `ListValue` is a wrapper around a repeated field of values.
//
// The JSON representation for `ListValue` is JSON array.
message ListValue {
  // Repeated field of dynamically typed values.
  repeated Value values = 1;
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
    use std::collections::HashSet;
    use crate::proto_common_types::{
        add_common_files, get_schema, get_schemas, CommonSchema, CommonType,
    };

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
        assert!(
            get_schema(&CommonSchema::CalendarPeriod).starts_with("// Copyright 2021 Google LLC")
        );
        assert!(get_schema(&CommonSchema::Color).starts_with("// Copyright 2021 Google LLC"));
        assert!(get_schema(&CommonSchema::Date).starts_with("// Copyright 2021 Google LLC"));
        assert!(get_schema(&CommonSchema::DateTime).starts_with("// Copyright 2021 Google LLC"));
        assert!(get_schema(&CommonSchema::DayOfWeek).starts_with("// Copyright 2021 Google LLC"));
        assert!(get_schema(&CommonSchema::Decimal).starts_with("// Copyright 2021 Google LLC"));
        assert!(get_schema(&CommonSchema::Expr).starts_with("// Copyright 2021 Google LLC"));
        assert!(get_schema(&CommonSchema::Fraction).starts_with("// Copyright 2021 Google LLC"),);
        assert!(get_schema(&CommonSchema::Interval).starts_with("// Copyright 2021 Google LLC"),);
        assert!(get_schema(&CommonSchema::LatLng).starts_with("// Copyright 2021 Google LLC"));
        assert!(
            get_schema(&CommonSchema::LocalizedText).starts_with("// Copyright 2021 Google LLC"),
        );
        assert!(get_schema(&CommonSchema::Money).starts_with("// Copyright 2021 Google LLC"));
        assert!(get_schema(&CommonSchema::Month).starts_with("// Copyright 2021 Google LLC"));
        assert!(get_schema(&CommonSchema::PhoneNumber).starts_with("// Copyright 2021 Google LLC"),);
        assert!(
            get_schema(&CommonSchema::PostalAddress).starts_with("// Copyright 2021 Google LLC")
        );
        assert!(get_schema(&CommonSchema::Quaternion).starts_with("// Copyright 2021 Google LLC"),);
        assert!(get_schema(&CommonSchema::TimeOfDay).starts_with("// Copyright 2021 Google LLC"));
        assert!(get_schema(&CommonSchema::Type).starts_with("type: google.api.Service"));
    }

    #[test]
    fn test_add_common_files_with_all_common_schemas() {
        let test_imports = vec![
            String::from("google/protobuf/any.proto"),
            String::from("google/protobuf/descriptor.proto"),
            String::from("google/protobuf/duration.proto"),
            String::from("google/protobuf/empty.proto"),
            String::from("google/protobuf/field_mask.proto"),
            String::from("google/protobuf/source_context.proto"),
            String::from("google/protobuf/struct.proto"),
            String::from("google/protobuf/timestamp.proto"),
            String::from("google/protobuf/type.proto"),
            String::from("google/protobuf/wrappers.proto"),
        ];

        let mut files: HashSet<String> = HashSet::new();

        add_common_files(&test_imports, &mut files);

        assert_eq!(10, files.len())
    }
}
