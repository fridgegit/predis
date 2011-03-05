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
  - Publish / Subscribe.
  - MULTI / EXEC transactions (not yet supported by Webdis).

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
    private $_parameters, $_resource, $_reader;

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
        $this->_reader = $this->initializeReader();
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
        $reader = phpiredis_reader_create();
        phpiredis_reader_set_status_handler($reader, $this->getStatusHandler());
        phpiredis_reader_set_error_handler($reader, $this->getErrorHandler(true));
        return $reader;
    }

    private function getStatusHandler() {
        return function($payload) {
            return $payload === 'OK' ? true : $payload;
        };
    }

    private function getErrorHandler($throwErrors) {
        if ($throwErrors) {
            return function($errorMessage) {
                throw new ServerException(substr($errorMessage, 4));
            };
        }
        else {
            return function($errorMessage) {
                return new ResponseError(substr($errorMessage, 4));
            };
        }
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

    private static function argumentsSerializer($str, $arg) {
        $str .= '/' . urlencode($arg);
        return $str;
    }

    public function executeCommand(ICommand $command) {
        $params = $this->_parameters;
        $arguments = array_reduce($command->getArguments(), 'self::argumentsSerializer');

        $requestUrl = "{$params->scheme}://{$params->host}:{$params->port}";
        $request = new HttpRequest($requestUrl, HttpRequest::METH_POST);
        $request->setBody(sprintf('%s%s.raw', $command->getId(), $arguments));
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
                phpiredis_reader_set_error_handler(
                    $this->_reader,
                    $this->getErrorHandler((bool) $value)
                );
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
