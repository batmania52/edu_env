{#-
  [Model Information]
  - Name: line_break_example
  - Developer: hjpark
  - Created At: 2026-03-19
  - Description: Example model demonstrating flexible line break rules for CASE, functions, and Window Functions.
  
  [Update History]
  - 2026-03-19: 최초 생성 (hjpark)
  - 2026-03-19: 윈도우 함수 정렬 예시 추가 (hjpark)
-#}

{%- set start, end = get_date_intervals() -%}

select customer_id
     , customer_name
     -- 원칙: 짧은 CASE 문이나 함수는 가독성을 위해 한 줄로 작성 (약 100자 이내)
     , case when total_spent > 1000 then 'vip' else 'normal' end as segment
     
     -- 예외: 로직이 길고 복잡하여 100자를 초과할 경우, 가독성을 위해 적절히 개행
     , case when total_spent > 5000 and last_order_date > '2026-01-01' then 'active_high_value'
            when total_spent > 5000 and last_order_date <= '2026-01-01' then 'churn_risk_high_value'
            else 'others'
       end                                                       as detailed_segment
       
     -- 윈도우 함수 예시: OVER 절 내부가 길어질 경우 PARTITION BY와 ORDER BY를 개행하여 정렬
     , row_number() over (partition by customer_id
                              order by last_order_date desc)     as last_order_seq
                              
     -- 복합 함수 예시 (한 줄 작성 원칙 준수)
     , round(total_spent / nullif(total_orders, 0), 2)::numeric  as avg_per_order
     
  from {{ ref('stg_customers') }}
 where registration_date >= '{{ start }}'::date
   and registration_date < '{{ end }}'::date
