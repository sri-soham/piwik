<?php
/**
 * Piwik - free/libre analytics platform
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 */
namespace Piwik\Db;

/**
 * Model objects that are to be instantiated through a factory class have to
 * implement this interface. Objects are created through dependency injection.
 * Some of these objects have instances of Model classes as arguments to the
 * parameter. Some of these Model classes are created by the Piwik\Db\Factory.
 * When the DI container is created these Model objects, they have to be
 * generated through the Factory. Since not all Model objects are created by
 * the Factory class at the moment (2015-11-28), we need a way of identifying
 * the models that are to be created through the Factory class. And we do that
 * by making the concerned Model class implement this interface.
 *
 * This interface is for class identification, it doesn't add any behaviour.
 */
interface FactoryCreated {}

