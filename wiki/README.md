# Sous wiki source

This directory is the local source for the Sous GitHub wiki. Page files are
markdown; navigation is driven by `_Sidebar.md`; the footer is `_Footer.md`.
The GitHub wiki at https://github.com/osvaldoandrade/sous/wiki is the
authoritative reader-facing surface; this directory is the writable source.

## Publishing

To publish changes from this directory to the GitHub wiki, run the publish
script from the repository root:

```bash
./wiki/publish.sh
```

The script clones the wiki repository into a temporary directory, syncs only
`.md` files from `wiki/` into it (deleting wiki files that are no longer
present locally), commits the result, and pushes to the wiki's `master`
branch. Markdown is the only file type published; HTML, CSS, and JavaScript
artifacts are not copied.

By default the script targets `osvaldoandrade/sous`. Pass an alternative
`owner/repo` as the first argument to publish to a fork or staging
repository:

```bash
./wiki/publish.sh myorg/sous-staging
```

## Page conventions

Page filenames map directly to wiki URL slugs. Cross-page links use the bare
page name without an extension, for example `[Developers-CLI](Developers-CLI)`.
`_Sidebar.md` controls the left-hand navigation and `_Footer.md` the page
footer; both are reserved filenames recognized by GitHub Wiki.

The set of canonical pages — including the Programming-Model, Developers,
Operators, and Reference families — is enumerated in the project plan and on
the wiki Home page.
