package com.intel.imllib.util

// 仅仅考虑 "IOB2" 标记的情况
case class Parameter(
                    feature_name_list:Array[String] = Array ("token", "CapWord", "AllCaps", "CapsMix", "AlphaDigitMix",
                      "AlphaDigit", "Hyphen", "InitHyphen", "EndHyphen", "Punctuation",
                      "Quote", "GreekLetter", "UpperLetter", "Numeral", "TwoNumeral","ContainSlaslT",
                      "LeftMarkChar", "RightMarkChar","EndDot","OneOrTwoLetter","AllDigit","Equal","Underline",
                      "Plus","word_length", "keywords_feature", "prefix3", "prefix4", "suffix3", "suffix4", "morph",
                      "s_morph", "left_boundary", "right_boundary", "Unary_feature", "Nested_feature", "StopWord_feature","CommenWord_feature", "Context_feature", "POS_feature",
                      "dict", "dict_U", "dict_N")
                    )
