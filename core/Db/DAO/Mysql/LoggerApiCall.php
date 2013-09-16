<?php /** * Piwik - Open source web analytics *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 * @category Piwik
 * @package Piwik
 */
namespace Piwik\Db\DAO\Mysql;

use Piwik\Db\DAO\Base;

/**
 * Logger Api Call
 *
 * Doesn't add any functionality. This has been created so that the
 * getTablesWithData and restoreDbTables of the IntegrationTestCase
 * have some classes for the logger_api_call table.
 *
 * @package Piwik
 * @subpackage Piwik_Db
 */
class LoggerApiCall extends Base
{
}
