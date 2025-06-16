from pyspark.sql.functions import col, isnan, when, count

def data_quality_checks(df):
    errors = []

    # 1. Check for required columns
    required_cols = ['Age', 'StateCode']
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        errors.append(f"Missing required columns: {missing_cols}")

    # 2. Check for nulls in critical columns
    null_counts = df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in required_cols]).collect()[0].asDict()
    for col_name, null_count in null_counts.items():
        if null_count > 0:
            errors.append(f"Column '{col_name}' has {null_count} null or NaN values")

    # 3. Validate Age range (e.g., 0 < Age < 120)
    invalid_ages = df.filter((col('Age') <= 0) | (col('Age') > 120)).count()
    if invalid_ages > 0:
        errors.append(f"Found {invalid_ages} rows with invalid Age values")

    # 4. Validate StateCode format (assumes 2 uppercase letters)
    invalid_states = df.filter(~col('StateCode').rlike("^[A-Z]{2}$")).count()
    if invalid_states > 0:
        errors.append(f"Found {invalid_states} rows with invalid StateCode")

    if errors:
        raise ValueError("Data Quality Checks Failed:\n" + "\n".join(errors))
    else:
        print("All Data Quality Checks Passed ✅")

    return True
