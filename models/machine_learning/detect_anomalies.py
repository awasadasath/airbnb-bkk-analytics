import pandas as pd
from sklearn.ensemble import IsolationForest

def model(dbt, session):
    dbt.config(
        materialized = "table",
        packages = ["pandas", "scikit-learn"]
    )

    df = dbt.ref("dim_listings").to_pandas()

    numerical_cols = ['PRICE', 'MINIMUM_NIGHTS', 'NUMBER_OF_REVIEWS', 'REVIEW_SCORES_RATING']
    categorical_cols = ['ROOM_TYPE']
    
    df[numerical_cols] = df[numerical_cols].fillna(0)

    # One-Hot Encoding
    X = pd.get_dummies(df[numerical_cols + categorical_cols], columns=categorical_cols)

    # Train (contamination=0.01)
    iso = IsolationForest(contamination=0.01, n_estimators=100, random_state=42)
    
    df['IS_ANOMALY'] = iso.fit_predict(X)

    # Return (-1 = ผิดปกติ)
    return df[df['IS_ANOMALY'] == -1][['LISTING_ID', 'PRICE', 'ROOM_TYPE', 'IS_ANOMALY']]