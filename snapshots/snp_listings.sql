{% snapshot snp_listings %}

{{
    config(
        target_schema='snapshots',     
        unique_key='listing_id',       
        
        strategy='check',              
        check_cols=[                   
            'price',
            'host_is_superhost',
            'host_response_time',
            'review_scores_rating',
            'number_of_reviews'
        ]
    )
}}

-- เลือกข้อมูลจาก Staging ที่ clean แล้ว
SELECT * FROM {{ ref('stg_listings') }}

{% endsnapshot %}