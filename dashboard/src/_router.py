import streamlit as st

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
