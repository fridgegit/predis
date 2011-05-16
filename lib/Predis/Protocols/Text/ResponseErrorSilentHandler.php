<?php

namespace Predis\Protocols\Text;

use Predis\Helpers;
use Predis\Protocols\IResponseHandler;
use Predis\Network\IConnectionComposable;

class ResponseErrorSilentHandler implements IResponseHandler {
    public function handle(IConnectionComposable $connection, $errorMessage) {
        return Helpers::handleRedisError($errorMessage);
    }
}
