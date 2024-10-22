import hmac
import streamlit as st

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


if not check_password():
    st.stop()


st.logo("dashboard/artifacts/pitchit_hq.png", size="large")

dashboards = dict(
    main=st.Page(
        "dashboards/main.py",
        title="Dashboard",
        icon=":material/dashboard:",
        default=True,
    ),
)

notebooks = dict(
    eda=st.Page(
        "notebooks/eda.py",
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
