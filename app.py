import streamlit as st
import pandas as pd
import io
import re
import json
from anthropic import Anthropic

# --- Configuration ---
CLAUDE_MODEL = "claude-sonnet-4-20250514"
CHUNK_SIZE = 80  # rows per Claude call

# --- Password Gate ---
def check_password():
    def password_entered():
        if st.session_state["password"] == st.secrets["password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input("Password", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.text_input("Password", type="password", on_change=password_entered, key="password")
        st.error("Incorrect password.")
        return False
    else:
        return True


# --- Claude Processing ---
def call_claude(client, chunk_csv: str, chunk_num: int, total_chunks: int) -> list:
    prompt = f"""
You are an expert in multifamily real estate underwriting. Standardize the rent roll data below.

REQUIRED OUTPUT COLUMNS:
Unit No | Unit Size (SF) | Market Rent (Monthly) | Effective Rent (Monthly) | Lease Start Date | Lease End Date | Tenant Name

STANDARDIZATION RULES:
1. One row per tenant. Combine all charge lines for the same unit into one row.
2. Effective Rent = the base rent charge ONLY (the primary "rent" line item).
   - ADD housing subsidy if present (charge codes: rentsub, hap, subsidy).
   - SUBTRACT employee discounts (empdisc) and move-in discounts (discnewm) if present.
   - IGNORE and DO NOT ADD any of the following to Effective Rent under any circumstances:
     deposits (security deposit, resident deposit, other deposit, any column with "deposit" in the name),
     utilities, water, electric, gas, trash, pest control,
     pet fees (petfee), parking fees (parkfee), amenity fees (amentfee),
     concessions, late fees, administrative fees, or any other ancillary charges.
   - If you are unsure whether a charge belongs, leave it out. When in doubt, exclude it.
3. If rents are annual, divide by 12. If rent/sf is given, multiply by unit size to get monthly rent.
4. Normalize all dates to MM/DD/YYYY. If date is missing, use null.
5. Vacant rows: include if market rent is shown. Set Effective Rent to null, Tenant Name to "VACANT".
6. Admin/Model units: include them. Set Effective Rent to null. Tenant Name = "ADMIN" or "MODEL".
7. Future/pending leases with no active rent charges: exclude.
8. Remove exact duplicate rows.
9. Do NOT include subtotals, section headers, summary rows, or footers.
10. Round all monetary values to 2 decimal places.

Return ONLY a valid JSON array. No markdown, no explanation, no code fences. Just the raw JSON array starting with [ and ending with ].

Input data (chunk {chunk_num} of {total_chunks}):
{chunk_csv}
"""

    response = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}]
    )

    raw = response.content[0].text.strip()

    # Strip markdown code fences if Claude added them
    raw = re.sub(r"^```(?:json)?\s*\n?", "", raw, flags=re.MULTILINE)
    raw = re.sub(r"\n?```\s*$", "", raw, flags=re.MULTILINE)
    raw = raw.strip()

    return json.loads(raw)


def standardize_rent_roll(df: pd.DataFrame) -> pd.DataFrame:
    client = Anthropic(api_key=st.secrets["anthropic_api_key"])

    total_rows = len(df)
    num_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
    all_results = []

    progress_bar = st.progress(0)
    status = st.empty()

    for i in range(num_chunks):
        start = i * CHUNK_SIZE
        end = min((i + 1) * CHUNK_SIZE, total_rows)
        chunk = df.iloc[start:end]

        status.info(f"Processing chunk {i+1} of {num_chunks} (rows {start+1}–{end})...")

        chunk_csv = chunk.to_csv(index=False)

        # Retry up to 3 times per chunk
        last_error = None
        for attempt in range(3):
            try:
                rows = call_claude(client, chunk_csv, i + 1, num_chunks)
                all_results.extend(rows)
                last_error = None
                break
            except json.JSONDecodeError as e:
                last_error = f"Invalid JSON returned by Claude (attempt {attempt+1}/3): {e}"
            except Exception as e:
                last_error = f"Error on chunk {i+1} (attempt {attempt+1}/3): {e}"

        if last_error:
            status.empty()
            st.error(last_error)
            return pd.DataFrame()

        progress_bar.progress((i + 1) / num_chunks)

    status.empty()

    if not all_results:
        return pd.DataFrame()

    result_df = pd.DataFrame(all_results)

    # Ensure all required columns exist
    required_columns = [
        "Unit No", "Unit Size (SF)", "Market Rent (Monthly)",
        "Effective Rent (Monthly)", "Lease Start Date", "Lease End Date", "Tenant Name"
    ]
    for col in required_columns:
        if col not in result_df.columns:
            result_df[col] = None

    result_df = result_df[required_columns]

    # Normalize dates
    for date_col in ["Lease Start Date", "Lease End Date"]:
        result_df[date_col] = pd.to_datetime(
            result_df[date_col], errors="coerce"
        ).dt.strftime("%m/%d/%Y")

    # Deduplicate on Unit No only — keeps current tenant, drops future/pending duplicates
    result_df.drop_duplicates(subset=["Unit No"], keep="first", inplace=True)
    result_df.reset_index(drop=True, inplace=True)

    return result_df


# --- Main App ---
if check_password():

    st.title("Rent Roll Standardizer")
    st.caption("Upload any broker Excel rent roll — get a clean, standardized output.")

    uploaded_file = st.file_uploader(
        "Upload Excel Rent Roll (.xlsx)",
        type=["xlsx"]
    )

    if uploaded_file:
        st.success(f"File uploaded: **{uploaded_file.name}**")

        if st.button("Standardize Rent Roll"):

            with st.spinner("Reading file..."):
                try:
                    original_df = pd.read_excel(uploaded_file, header=None)
                except Exception as e:
                    st.error(f"Could not read the file: {e}")
                    st.stop()

            st.write(f"**Raw file:** {len(original_df)} rows × {original_df.shape[1]} columns")

            standardized_df = standardize_rent_roll(original_df)

            if standardized_df.empty:
                st.error("Standardization failed or returned no data. Please check the file and try again.")
                st.stop()

            st.success("✅ Standardization complete!")

            # --- Reconciliation ---
            st.subheader("Reconciliation")

            orig_rows    = len(original_df)
            clean_rows   = len(standardized_df)
            occupied     = standardized_df[
                ~standardized_df["Tenant Name"].str.upper().isin(["VACANT", "ADMIN", "MODEL"])
                & standardized_df["Tenant Name"].notna()
            ]
            vacant       = standardized_df[standardized_df["Tenant Name"].str.upper() == "VACANT"]
            clean_mkt    = pd.to_numeric(standardized_df["Market Rent (Monthly)"], errors="coerce").sum()
            avg_eff_rent = pd.to_numeric(standardized_df["Effective Rent (Monthly)"], errors="coerce").mean()
            missing_cols = [c for c in ["Unit No", "Unit Size (SF)", "Market Rent (Monthly)",
                                         "Effective Rent (Monthly)", "Lease Start Date",
                                         "Lease End Date", "Tenant Name"]
                            if c not in standardized_df.columns]

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Rows (Cleaned)", clean_rows)
            col2.metric("Occupied Tenants",     len(occupied))
            col3.metric("Vacant Units",          len(vacant))
            col4.metric("Avg Effective Rent",    f"${avg_eff_rent:,.0f}")

            st.write("")

            # Validation checks
            if missing_cols:
                st.warning(f"⚠️ Missing columns: {', '.join(missing_cols)}")
            else:
                st.success("✅ All required columns present")

            if clean_rows < orig_rows * 0.3:
                st.warning(
                    f"⚠️ Cleaned output has {clean_rows} rows vs {orig_rows} raw rows. "
                    "This is expected if the raw file has many header/footer/summary rows. "
                    "Please review the preview below."
                )
            else:
                st.success(f"✅ Row count looks reasonable ({clean_rows} cleaned from {orig_rows} raw rows)")

            st.write(f"**Total Market Rent (cleaned):** ${clean_mkt:,.2f}")
            st.write(f"**Occupied tenants:** {len(occupied)} &nbsp;|&nbsp; **Vacant units:** {len(vacant)}")

            # --- Preview ---
            st.subheader("Preview")
            st.dataframe(standardized_df, use_container_width=True, height=400)

            # --- Download ---
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                standardized_df.to_excel(writer, index=False, sheet_name="Standardized Rent Roll")
            output.seek(0)

            base_name = uploaded_file.name.replace(".xlsx", "")
            st.download_button(
                label="⬇️ Download Standardized Rent Roll",
                data=output.getvalue(),
                file_name=f"{base_name}_standardized.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    else:
        st.info("Please upload an Excel file to begin.")
