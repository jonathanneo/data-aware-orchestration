from setuptools import find_packages, setup

setup(
    name="assets_modern_data_stack",
    packages=find_packages(exclude=["assets_modern_data_stack_tests"]),
    package_data={"assets_modern_data_stack": ["dbt_project_1/*", "dbt_project_2/*"]},
    install_requires=[
        "dagster",
        "dagster-airbyte",
        "dagster-managed-elements",
        "dagster-dbt",
        "dagster-postgres",
        "pandas",
        "dbt-core",
        "dbt-postgres",
    ],
    extras_require={"dev": ["dagit", "pytest", "black"]},
)
