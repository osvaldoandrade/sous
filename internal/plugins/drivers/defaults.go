package drivers

import (
	_ "github.com/osvaldoandrade/sous/internal/plugins/authn/tikti"
	_ "github.com/osvaldoandrade/sous/internal/plugins/messaging/codeq"
	_ "github.com/osvaldoandrade/sous/internal/plugins/persistence/kvrocks"
	_ "github.com/osvaldoandrade/sous/internal/plugins/secrets/memory"
	_ "github.com/osvaldoandrade/sous/internal/plugins/secrets/vault"
)
