<?php

namespace Predis\Protocols\Text;

use Predis\Helpers;
use Predis\Protocols\IResponseHandler;
use Predis\Network\IConnectionComposable;

class ResponseErrorHandler implements IResponseHandler {
    public function handle(IConnectionComposable $connection, $errorMessage) {
        Helpers::handleRedisError($errorMessage, true);
    }
}
