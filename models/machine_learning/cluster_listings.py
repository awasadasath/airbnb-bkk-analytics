import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

def model(dbt, session):
    dbt.config(
        materialized = "table",
        packages = ["pandas", "scikit-learn"]
    )

    df = dbt.ref("dim_listings").to_pandas()

    numerical_cols = ['PRICE', 'REVIEW_SCORES_RATING', 'ACCOMMODATES', 'NUMBER_OF_REVIEWS']
    categorical_cols = ['ROOM_TYPE']
    
    df[numerical_cols] = df[numerical_cols].fillna(0)

    # 1. One-Hot Encoding
    df_encoded = pd.get_dummies(df[numerical_cols + categorical_cols], columns=categorical_cols)
    
    # 2. Scale Data 
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df_encoded)

    # 3. Train (แบ่ง 4 กลุ่ม)
    kmeans = KMeans(n_clusters=4, random_state=42)
    df['CLUSTER_LABEL'] = kmeans.fit_predict(X_scaled)

    pca = PCA(n_components=2) # ย่อเหลือ 2 มิติ (X, Y)
    pca_components = pca.fit_transform(X_scaled)
    
    # เก็บค่าพิกัดใหม่ลงใน DataFrame
    df['PCA_X'] = pca_components[:, 0]
    df['PCA_Y'] = pca_components[:, 1]

    # 6. Return Result
    # ส่งคืนคอลัมน์ที่จำเป็น + ผลลัพธ์ PCA
    return df[['LISTING_ID', 'PRICE', 'ROOM_TYPE', 'CLUSTER_LABEL', 'PCA_X', 'PCA_Y']]