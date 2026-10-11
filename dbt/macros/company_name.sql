{#-
    Normalise a company name for matching: upper case, "&" to "AND", punctuation
    removed, then legal suffixes stripped. "JPMORGAN CHASE & CO." and the FDIC's
    "JPMORGAN CHASE&CO" both become "JPMORGAN CHASE".
-#}
{% macro normalize_company_name(expression) -%}
    {%- set suffix = '( NATIONAL ASSOCIATION| N A| NA| INC| LLC| L L C| LP| L P| LTD| CORPORATION| CORP| COMPANY| CO| THE| AND)$' -%}
    {%- set cleaned -%}
        trim({{ regex_replace(
            regex_replace("replace(upper(" ~ expression ~ "), '&', ' AND ')", '[^A-Z0-9 ]', ' '),
            ' +', ' '
        ) }})
    {%- endset -%}
    {%- set once = regex_replace(cleaned, suffix, '') -%}
    {%- set twice = regex_replace(once, suffix, '') -%}
    {%- set thrice = regex_replace(twice, suffix, '') -%}
    nullif(trim({{ regex_replace(thrice, '^THE ', '') }}), '')
{%- endmacro %}
