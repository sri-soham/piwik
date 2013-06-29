<?php /** * Piwik - Open source web analytics *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 * @category Piwik
 * @package Piwik
 */

/**
 * Logger Message
 *
 * Doesn't add any functionality. This has been created so that the
 * getTablesWithData and restoreDbTables of the IntegrationTestCase
 * have some classes for the logger_message table.
 *
 * @package Piwik
 * @subpackage Piwik_Db
 */
class Piwik_Db_DAO_LoggerException extends Piwik_Db_DAO_Base
{
}
