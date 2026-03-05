import streamlit as st
import pandas as pd
import io
import os
from anthropic import Anthropic
import json

# --- Configuration ---
CLAUDE_MODEL = "claude-3-opus-20240229" # Or other suitable Claude model
CHUNK_SIZE = 100 # Number of rows per chunk for Claude processing

# --- Security: Simple Password Gate ---
def check_password():
    """Returns `True` if the user enters the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == st.secrets["password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don\"t store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        return False
    elif not st.session_state["password_correct"]:
        # Password not correct, show input again
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        st.error("😕 Password incorrect")
        return False
    else:
        # Password correct.
        return True


# --- Claude AI Processing Function ---
def standardize_rent_roll_with_claude(df: pd.DataFrame) -> pd.DataFrame:
    client = Anthropic(api_key=st.secrets["anthropic_api_key"])

    standardized_dfs = []
    total_rows = len(df)
    num_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE

    for i in range(num_chunks):
        start_row = i * CHUNK_SIZE
        end_row = min((i + 1) * CHUNK_SIZE, total_rows)
        chunk_df = df.iloc[start_row:end_row]

        st.info(f"Processing chunk {i+1}/{num_chunks} (rows {start_row+1}-{end_row})...")

        # Convert chunk to a string format suitable for Claude
        # Using to_csv for simplicity, but could be more structured (e.g., JSON list of dicts)
        chunk_csv = chunk_df.to_csv(index=False)

        prompt_message = f"""
        You are an expert in multifamily real estate underwriting. Your task is to standardize rent roll data.
        The user will provide a CSV string representing a portion of a rent roll. Convert this data into a standardized one-line-per-tenant format.

        Here are the required output columns and standardization rules:

        REQUIRED STANDARD OUTPUT FORMAT (JSON array of objects, each object is a tenant):
        [{{"Unit No": "string", "Unit Size (SF)": "number", "Market Rent (Monthly)": "number", "Effective Rent (Monthly)": "number", "Lease Start Date": "MM/DD/YYYY", "Lease End Date": "MM/DD/YYYY", "Tenant Name": "string"}}]

        STANDARDIZATION RULES:
        - Each tenant must appear on only one row.
        - Convert any annual rents to monthly rents. If a rent column name suggests annual (e.g., \'Annual Rent\'), divide by 12.
        - If rent per square foot is given, calculate the total rent. Look for columns like \'Rent/SF\' and \'Unit Size (SF)\'.
        - Normalize all dates to MM/DD/YYYY format. Handle various date formats (e.g., YYYY-MM-DD, M/D/YY).
        - Remove duplicate rows based on a combination of \'Unit No\' and \'Tenant Name\'.
        - Ignore fully vacant rows unless market rent is provided. A row is fully vacant if \'Tenant Name\' is empty/null and \'Market Rent (Monthly)\' is also empty/null or zero.
        - Admin/model units must be included.
        - Ensure all required columns are present in the output. If a column is missing from the input and cannot be derived, use null or an appropriate default.

        Input Rent Roll CSV Chunk:
        ```csv
        {chunk_csv}
        ```

        Please return ONLY the JSON array of standardized tenant objects. Do not include any other text or explanation.
        """

        try:
            response = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=4000,
                messages=[
                    {"role": "user", "content": prompt_message}
                ]
            )
            claude_output = response.content[0].text
            
            # Attempt to parse JSON response
            standardized_data_list = json.loads(claude_output)
            chunk_standardized_df = pd.DataFrame(standardized_data_list)
            standardized_dfs.append(chunk_standardized_df)

        except json.JSONDecodeError:
            st.error(f"Claude returned invalid JSON for chunk {i+1}. Retrying or displaying error.")
            # Implement retry logic here if needed
            st.exception(f"Invalid JSON from Claude: {claude_output}")
            return pd.DataFrame() # Return empty DataFrame on critical error
        except Exception as e:
            st.error(f"Error processing chunk {i+1} with Claude: {e}")
            st.exception(e)
            return pd.DataFrame() # Return empty DataFrame on critical error

    if not standardized_dfs:
        return pd.DataFrame()

    # Combine all standardized chunks
    final_standardized_df = pd.concat(standardized_dfs, ignore_index=True)

    # --- Post-processing and Validation (after combining all chunks) ---
    # Ensure correct columns and data types
    required_columns = [
        "Unit No", "Unit Size (SF)", "Market Rent (Monthly)",
        "Effective Rent (Monthly)", "Lease Start Date", "Lease End Date", "Tenant Name"
    ]
    for col in required_columns:
        if col not in final_standardized_df.columns:
            final_standardized_df[col] = None # Add missing columns

    # Reorder columns to match the required format
    final_standardized_df = final_standardized_df[required_columns]

    # Convert date columns to datetime objects, then format to MM/DD/YYYY
    for date_col in ["Lease Start Date", "Lease End Date"]:
        final_standardized_df[date_col] = pd.to_datetime(final_standardized_df[date_col], errors=\'coerce\').dt.strftime(\'%m/%d/%Y\')

    # Remove duplicate rows again after combining, based on Unit No and Tenant Name
    final_standardized_df.drop_duplicates(subset=["Unit No", "Tenant Name"], inplace=True)

    return final_standardized_df


# --- Main Streamlit App Logic ---
if check_password():
    st.title("Rent Roll Standardizer")

    uploaded_file = st.file_uploader("Upload an Excel Rent Roll File (.xlsx)", type=["xlsx"])

    if uploaded_file is not None:
        st.info("File uploaded successfully. Click \'Standardize Rent Roll\' to process.")

        if st.button("Standardize Rent Roll"):
            with st.spinner("Processing rent roll..."):
                try:
                    original_df = pd.read_excel(uploaded_file)
                    st.write("Original Rent Roll (first 5 rows):")
                    st.dataframe(original_df.head())

                    # Perform standardization using Claude
                    standardized_df = standardize_rent_roll_with_claude(original_df)

                    if not standardized_df.empty:
                        st.success("Rent roll standardized successfully!")
                        st.write("Standardized Rent Roll (first 5 rows):")
                        st.dataframe(standardized_df.head())

                        # --- Reconciliation Checks ---
                        st.subheader("Reconciliation Checks")
                        original_rows = len(original_df)
                        cleaned_rows = len(standardized_df)
                        st.write(f"- Original rows: {original_rows}")
                        st.write(f"- Cleaned rows: {cleaned_rows}")

                        if cleaned_rows == 0:
                            st.warning("No rows were returned after standardization. Please check the input file.")
                        elif cleaned_rows < original_rows:
                            st.warning(f"Number of cleaned rows ({cleaned_rows}) is less than original rows ({original_rows}). This might be due to filtering vacant/duplicate rows.")
                        elif cleaned_rows > original_rows:
                            st.warning(f"Number of cleaned rows ({cleaned_rows}) is greater than original rows ({original_rows}). This is unexpected and might indicate an issue.")
                        else:
                            st.info("Number of rows after cleaning matches original (or is within expected range).")

                        # Check for missing required columns
                        required_columns = [
                            "Unit No", "Unit Size (SF)", "Market Rent (Monthly)",
                            "Effective Rent (Monthly)", "Lease Start Date", "Lease End Date", "Tenant Name"
                        ]
                        missing_cols = [col for col in required_columns if col not in standardized_df.columns or standardized_df[col].isnull().all()]
                        if missing_cols:
                            st.warning(f"Missing or entirely null required columns: {\', \'.join(missing_cols)}")
                        else:
                            st.info("All required columns are present.")

                        # Provide download button for the standardized file
                        output = io.BytesIO()
                        with pd.ExcelWriter(output, engine=\'xlsxwriter\') as writer:
                            standardized_df.to_excel(writer, index=False, sheet_name=\'Standardized Rent Roll\')
                        processed_data = output.getvalue()

                        st.download_button(
                            label="Download Standardized Rent Roll",
                            data=processed_data,
                            file_name="standardized_rent_roll.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        )
                    else:
                        st.error("Standardization failed or returned no data.")

                except Exception as e:
                    st.error(f"An error occurred during file processing: {e}")
                    st.exception(e)

    else:
        st.info("Please upload an Excel file to begin.")

