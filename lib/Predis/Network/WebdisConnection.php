<?php
/*
This class implements a Predis connection that actually talks with Webdis
(http://github.com/nicolasff/webdis) instead of connecting directly to Redis.
It relies on the http PECL extension to communicate with the web server and the
phpiredis extension to parse the protocol of the replies returned in the http 
response bodies.

Since this connection class is highly experimental, some features have not been
implemented yet (or they simply cannot be implemented at all). Here is a list:

  - Pipelining commands.
  - Sharding (it cannot be used with a clustered connection).
  - Publish / Subscribe.
  - MULTI / EXEC transactions (not yet supported by Webdis).

Webdis currently needs to be patched in formats/raw.c to make it return replies
that are correctly formatted according to the Redis protocol specifications.
*/

namespace Predis\Network;

use HttpRequest;
use Predis\ResponseError;
use Predis\ClientException;
use Predis\ServerException;
use Predis\ConnectionParameters;
use Predis\CommunicationException;
use Predis\Commands\ICommand;

const ERR_MSG_EXTENSION = 'The %s extension must be loaded in order to be able to use this connection class';

class WebdisConnection implements IConnectionSingle {
    private $_parameters, $_resource, $_reader, $_throwErrors;

    private static function throwNotYetImplementedException($class, $function) {
        throw new \RuntimeException("The method $class::$function() has not been implemented yet");
    }

    private static function throwNotImplementedException($class, $function) {
        throw new \RuntimeException("The method $class::$function() is not implemented");
    }

    public function __construct(ConnectionParameters $parameters) {
        $this->checkExtensions();
        if ($parameters->scheme !== 'http') {
            throw new \InvalidArgumentException("Invalid scheme: {$parameters->scheme}");
        }
        $this->_parameters = $parameters;
        $this->_throwErrors = true;
        $this->initializeReader();
    }

    public function __destruct() {
        phpiredis_reader_destroy($this->_reader);
    }

    private function checkExtensions() {
        if (!class_exists("HttpRequest")) {
            throw new ClientException(sprintf(ERR_MSG_EXTENSION, 'http'));
        }
        if (!function_exists('phpiredis_reader_create')) {
            throw new ClientException(sprintf(ERR_MSG_EXTENSION, 'phpiredis'));
        }
    }

    private function initializeReader() {
        $this->_reader = phpiredis_reader_create();
        phpiredis_reader_set_status_handler($this->_reader, $this->getStatusHandler());
        phpiredis_reader_set_error_handler($this->_reader, $this->getErrorHandler());
    }

    private function getStatusHandler() {
        return function($payload) {
            switch ($payload) {
                case 'OK':
                    return true;
                default:
                    return $payload;
            }
        };
    }

    private function getErrorHandler() {
        if ($this->_throwErrors) {
            return function($errorMessage) {
                throw new ServerException(substr($errorMessage, 4));
            };
        }
        return function($errorMessage) {
            return new ResponseError(substr($errorMessage, 4));
        };
    }

    public function connect() {
        // NOOP
    }

    public function disconnect() {
        // NOOP
    }

    public function isConnected() {
        return true;
    }

    public function writeCommand(ICommand $command) {
        self::throwNotYetImplementedException(__CLASS__, __FUNCTION__);
    }

    public function readResponse(ICommand $command) {
        self::throwNotYetImplementedException(__CLASS__, __FUNCTION__);
    }

    public function executeCommand(ICommand $command) {
        $params = $this->_parameters;
        $requestUrl = sprintf("%s://%s:%d/%s%s.raw",
            $params->scheme, $params->host, $params->port, $command->getId(),
            array_reduce($command->getArguments(), function($str, $arg) {
                $str .= '/' . urlencode($arg);
                return $str;
            })
        );

        $request = new HttpRequest($requestUrl);
        $request->send();

        phpiredis_reader_feed($this->_reader, $request->getResponseBody());
        $reply = phpiredis_reader_get_reply($this->_reader);
        return isset($reply->skipParse) ? $reply : $command->parseResponse($reply);
    }

    public function getResource() {
        self::throwNotImplementedException(__CLASS__, __FUNCTION__);
    }

    public function getParameters() {
        return $this->_parameters;
    }

    public function setProtocolOption($option, $value) {
        switch ($option) {
            case 'throw_errors':
                $this->_throwErrors = (bool) $value;
                phpiredis_reader_set_error_handler($this->_reader, $this->getErrorHandler());
                break;
        }
    }

    public function pushInitCommand(ICommand $command) {
        self::throwNotImplementedException(__CLASS__, __FUNCTION__);
    }

    public function read() {
        self::throwNotImplementedException(__CLASS__, __FUNCTION__);
    }

    public function __toString() {
        return "{$this->_parameters->host}:{$this->_parameters->port}";
    }
}
