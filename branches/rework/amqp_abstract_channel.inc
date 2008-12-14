<?php
  /**
   * Simple AMQP client library for AMQP for protocol version 0.8
   *
   * http://code.google.com/p/php-amqplib/
   * Vadim Zaliva <lord@crocodile.org>
   *
   */

class AbstractChannel
{

    public function __construct($connection, $channel_id)
    {
        $this->connection = $connection;
        $this->channel_id = $channel_id;
        $connection->channels[$channel_id] = $this;
        $this->method_queue = array(); // Higher level queue for methods
        $this->auto_decode = false;
    }

    protected function send_method($method_sig, $args='', $content=NULL)
    {
        if(is_a($args, "AMQPWriter"))
            $args = $args->getvalue();

        $this->connection->method_writer->write_method($this->channel_id, $method_sig, $args, $content);
    }


    /**
     * Wait for some expected AMQP methods and dispatch to them.
     */
    protected function wait($allowed_methods=NULL)
    {
        if($allowed_methods)
            debug_msg("waiting for " . implode(", ", $allowed_methods));
        else
            debug_msg("waiting for any method");

        $a = $this->connection->wait_method($this->channel_id, $allowed_methods);
        // FIXME: trying to do python-style tuple unpacking here, not
        // sure what the PHP equivalent is.
        $method_sig = $a[0];
        $args = $a[1];
        $content = $a[2];

        if (($content != NULL) and $this->auto_decode and isset($content->content_encoding))
        {
            try
            {
                $msg->body = $msg->body->decode($msg->content_encoding);
            } catch (Exception $e) {
                debug_msg("Ignoring body decoding exception: " . $e->getMessage());
            }
        }

        if(!array_key_exists($method_sig, $this->METHOD_MAP))
            throw new Exception("Unknown AMQP method $method_sig");

        $amqp_method = $this->METHOD_MAP[$method_sig];
        if($content == NULL)
            return call_user_func(array($this,$amqp_method), $args);
        else
            return call_user_func(array($this,$amqp_method), $args, $content);

    }
}

?>
