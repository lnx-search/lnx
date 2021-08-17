# Securing Your System
Generally, it is highly recommended that you enable token authorization for lnx
although you might not intend for it to be exposed to the world, mistakes do happen.

Luckily for you lnx provides you with a simple a customisable authorization permission
system.

## Endpoints
All of these endpoints require the super use key if auth is enabled.

See [*the authentication key section*](/getting_started/running.html#authentication-key)

Any displayed query parameters are required parameters, scroll further down
for more info.

```
POST /admin/tokens/create?username=<str>
```

```
POST /admin/tokens/revoke?token=<token>
```

```
POST /admin/tokens/permissions?token=<token>
```

```
DELETE /admin/tokens/clear
```

### Extra Query Parameters
The `create` and  `permissions` both have a set of optional
parameters and they match up with the permissions (`search`, `indexes`, `documents`)
passing these parameters to these endpoints explicitly set or unset the permissions
of the access token (by default they are all `false`).

## Permissions
The following permissions each have their own sections and endpoints that they
affect, generally it is best to trust no one and only grant people
the permissions they absolutely need **ESPECIALLY** the indexes permission which
can have the potential to crash your server if abused.

#### Search Permission
`search` - This grants access to the endpoints described in the
[**Searching An Index**](/getting_started/searching.html) section.

#### Indexes Permission
`indexes` - The most dangerous permission, this grants access to any operations
listed in the [**Indexes**](/getting_started/indexes.html) section

#### Documents Permission
`documents` - This grants access to the endpoints described in the 
[**Documents**](/getting_started/documents.html) section.
