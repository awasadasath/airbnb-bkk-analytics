import pandas as pd
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split

def model(dbt, session):
    dbt.config(
        materialized = "table",
        packages = ["pandas", "xgboost", "scikit-learn"]
    )

    df = dbt.ref("dim_listings").to_pandas()

    # 1. Features
    numerical_cols = ['ACCOMMODATES', 'BEDROOMS', 'BEDS', 'REVIEW_SCORES_RATING', 'NUMBER_OF_REVIEWS', 'MINIMUM_NIGHTS']
    categorical_cols = ['ROOM_TYPE', 'NEIGHBOURHOOD_NAME', 'PROPERTY_TYPE'] 
    target = 'PRICE'

    # 2. Clean Data
    df[numerical_cols] = df[numerical_cols].fillna(0)
    df = df.dropna(subset=[target])

    # 3. One-Hot Encoding
    df_processed = pd.get_dummies(df[numerical_cols + categorical_cols], columns=categorical_cols, drop_first=True, dummy_na=False)
    
    # Handle columns names to avoid errors with special characters
    df_processed.columns = [c.replace(' ', '_').replace('/', '_').replace('-', '_') for c in df_processed.columns]

    # 4. Train
    X = df_processed
    y = df[target]

    # แบ่งข้อมูล: Train 80%, Test 20%
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
    model.fit(X_train, y_train)

    df['PREDICTED_PRICE'] = model.predict(X)

    return df[['LISTING_ID', 'PRICE', 'PREDICTED_PRICE']]