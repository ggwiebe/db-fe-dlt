# nyc-ent-fe
Shared repository for demos, projects, and scratch work to be easily shared between all NYC SAs

This is an internal repo, meaning all Databricks employees can view this.

General guidelines:
- Please do not make breaking changes directly on any demos or other material in `main` that could be shared live with customers. 
- If you want to modify someone's work, create a new branch, make the changes, and test before merging to main.
- If you want to run demos and be sure you have your own copy of the notebook (when using a shared demo workspace), create a personal branch off main and pull regularly.

Tips for making good demo notebooks:
- You should be able to "Run All" multiple times and get the same result each time (entire notebook idempotency)
- Create idempotent and self-contained setup cells which can easily be hidden; hide boilerplate when possible
- Read datasets preloaded in e2-demo-field-eng and update them in a temporary location
- Parameterize metastore and storage paths with personal info so multiple people can run the notebook on the same workspace without interefering
