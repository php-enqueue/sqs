<?php

namespace Enqueue\Sqs\Tests;

use Enqueue\Sqs\SqsConnectionFactory;
use Enqueue\Test\ClassExtensionTrait;
use PHPUnit\Framework\TestCase;

/**
 * The class contains the factory tests dedicated to configuration.
 */
class SqsConnectionFactoryConfigTest extends TestCase
{
    use ClassExtensionTrait;

    public function testThrowNeitherArrayStringNorNullGivenAsConfig()
    {
        $this->expectException(\LogicException::class);
        $this->expectExceptionMessage('The config must be either an array of options, a DSN string, null or instance of Aws\Sqs\SqsClient');

        new SqsConnectionFactory(new \stdClass());
    }

    public function testThrowIfSchemeIsNotAmqp()
    {
        $this->expectException(\LogicException::class);
        $this->expectExceptionMessage('The given scheme protocol "http" is not supported. It must be "sqs"');

        new SqsConnectionFactory('http://example.com');
    }

    public function testThrowIfDsnCouldNotBeParsed()
    {
        $this->expectException(\LogicException::class);
        $this->expectExceptionMessage('The DSN is invalid.');

        new SqsConnectionFactory('foo');
    }

    /**
     * @dataProvider provideConfigs
     *
     * @param mixed $config
     * @param mixed $expectedConfig
     */
    public function testShouldParseConfigurationAsExpected($config, $expectedConfig)
    {
        $factory = new SqsConnectionFactory($config);

        $this->assertAttributeEquals($expectedConfig, 'config', $factory);
    }

    public static function provideConfigs()
    {
        yield [
            null,
            [
                'key' => null,
                'secret' => null,
                'token' => null,
                'region' => null,
                'retries' => 3,
                'version' => '2012-11-05',
                'lazy' => true,
                'endpoint' => null,
            ],
        ];

        yield [
            'sqs:',
            [
                'key' => null,
                'secret' => null,
                'token' => null,
                'region' => null,
                'retries' => 3,
                'version' => '2012-11-05',
                'lazy' => true,
                'endpoint' => null,
            ],
        ];

        yield [
            [],
            [
                'key' => null,
                'secret' => null,
                'token' => null,
                'region' => null,
                'retries' => 3,
                'version' => '2012-11-05',
                'lazy' => true,
                'endpoint' => null,
            ],
        ];

        yield [
            'sqs:?key=theKey&secret=theSecret&token=theToken&lazy=0',
            [
                'key' => 'theKey',
                'secret' => 'theSecret',
                'token' => 'theToken',
                'region' => null,
                'retries' => 3,
                'version' => '2012-11-05',
                'lazy' => false,
                'endpoint' => null,
            ],
        ];

        yield [
            ['dsn' => 'sqs:?key=theKey&secret=theSecret&token=theToken&lazy=0'],
            [
                'key' => 'theKey',
                'secret' => 'theSecret',
                'token' => 'theToken',
                'region' => null,
                'retries' => 3,
                'version' => '2012-11-05',
                'lazy' => false,
                'endpoint' => null,
            ],
        ];

        yield [
            ['key' => 'theKey', 'secret' => 'theSecret', 'token' => 'theToken', 'lazy' => false],
            [
                'key' => 'theKey',
                'secret' => 'theSecret',
                'token' => 'theToken',
                'region' => null,
                'retries' => 3,
                'version' => '2012-11-05',
                'lazy' => false,
                'endpoint' => null,
            ],
        ];

        yield [
            [
                'key' => 'theKey',
                'secret' => 'theSecret',
                'token' => 'theToken',
                'lazy' => false,
                'endpoint' => 'http://localstack:1111',
            ],
            [
                'key' => 'theKey',
                'secret' => 'theSecret',
                'token' => 'theToken',
                'region' => null,
                'retries' => 3,
                'version' => '2012-11-05',
                'lazy' => false,
                'endpoint' => 'http://localstack:1111',
            ],
        ];
    }
}
