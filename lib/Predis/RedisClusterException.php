<?php

namespace Predis;

class RedisClusterException extends ServerException implements IReplyObject {
    public function getMoveDetails() {
        return explode(' ', $this->getMessage(), 3);
    }
}
