import hmac
import streamlit as st
from os import environ
from dotenv import load_dotenv

load_dotenv()

def check_password() -> bool:
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if hmac.compare_digest(st.session_state["password"], st.secrets["password"]):
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don't store the password.
        else:
            st.session_state["password_correct"] = False

    # Return True if the password is validated.
    if st.session_state.get("password_correct", False):
        return True

    # Show input for password.
    st.text_input(
        "Password", type="password", on_change=password_entered, key="password"
    )
    if "password_correct" in st.session_state:
        st.error("😕 Password incorrect")
    return False

# Check if authentication is required based on environment
REQUIRE_AUTH = st.secrets.get("REQUIRE_AUTH", False)
if environ.get("DEVELOPMENT") == "local":
    REQUIRE_AUTH = False

if REQUIRE_AUTH and not check_password():
    st.stop()

st.logo("dashboard/artifacts/pitchit_hq.png", size="large")

dashboards = dict(
    main=st.Page(
        "dashboards/main.py",
        title="Dashboard",
        icon=":material/dashboard:",
        default=True,
    ),
    predictions=st.Page(
        "dashboards/predictions.py",
        title="Predictions",
        icon=":material/insert_drive_file:",
    ),
)

notebooks = dict(
    eda=st.Page(
        "notebooks/eda.py",
        icon=":material/insert_drive_file:",
    ),
    init=st.Page(
        "notebooks/init.py",
        icon=":material/insert_drive_file:",
    ),
)

pg = st.navigation(
    {
        "Dashboards": [*dashboards.values()],
        "Notebooks": [*notebooks.values()],
    }
)

pg.run()
