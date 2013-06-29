<?php
/**
 * Piwik - Open source web analytics
 *
 * @link     http://piwik.org
 * @license  http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 * @category Piwik_Plugins
 * @package  Piwik_Dashboard
 */

/**
 * Dashboard Controller
 *
 * @package Piwik_Dashboard
 */
class Piwik_Dashboard_Controller extends Piwik_Controller
{
    /**
     * @var Piwik_Dashboard
     */
    private $dashboard;

    protected function init()
    {
        parent::init();

        $this->dashboard = new Piwik_Dashboard();
    }

    protected function _getDashboardView($template)
    {
        $view = Piwik_View::factory($template);
        $this->setGeneralVariablesView($view);

        $view->availableWidgets = Piwik_Common::json_encode(Piwik_GetWidgetsList());
        $view->availableLayouts = $this->getAvailableLayouts();

        $view->dashboardId = Piwik_Common::getRequestVar('idDashboard', 1, 'int');
        $view->dashboardLayout = $this->getLayout($view->dashboardId);

        return $view;
    }

    public function embeddedIndex()
    {
        $view = $this->_getDashboardView('index');

        echo $view->render();
    }

    public function index()
    {
        $view = $this->_getDashboardView('standalone');
        $view->dashboards = array();
        if (!Piwik::isUserIsAnonymous()) {
            $login = Piwik::getCurrentUserLogin();

            $view->dashboards = $this->dashboard->getAllDashboards($login);
        }
        echo $view->render();
    }

    public function getAvailableWidgets()
    {
        $this->checkTokenInUrl();

        Piwik_DataTable_Renderer_Json::sendHeaderJSON();
        echo Piwik_Common::json_encode(Piwik_GetWidgetsList());
    }

    public function getDashboardLayout()
    {
        $this->checkTokenInUrl();

        $idDashboard = Piwik_Common::getRequestVar('idDashboard', 1, 'int');

        $layout = $this->getLayout($idDashboard);

        echo $layout;
    }

    /**
     * Resets the dashboard to the default widget configuration
     */
    public function resetLayout()
    {
        $this->checkTokenInUrl();
        $layout = $this->dashboard->getDefaultLayout();
        $idDashboard = Piwik_Common::getRequestVar('idDashboard', 1, 'int');
        if (Piwik::isUserIsAnonymous()) {
            $session = new Piwik_Session_Namespace("Piwik_Dashboard");
            $session->dashboardLayout = $layout;
            $session->setExpirationSeconds(1800);
        } else {
            $this->saveLayoutForUser(Piwik::getCurrentUserLogin(), $idDashboard, $layout);
        }
    }

    /**
     * Records the layout in the DB for the given user.
     *
     * @param string $login
     * @param int $idDashboard
     * @param string $layout
     */
    protected function saveLayoutForUser($login, $idDashboard, $layout)
    {
        $UserDashboard = Piwik_Db_Factory::getDAO('user_dashboard');
        $UserDashboard->saveLayout($login, $idDashboard, $layout);
    }

    /**
     * Updates the name of a dashboard
     *
     * @param string $login
     * @param int $idDashboard
     * @param string $name
     */
    protected function updateDashboardName($login, $idDashboard, $name)
    {
        $UserDashboard = Piwik_Db_Factory::getDAO('user_dashboard');
        $UserDashboard->updateName($name, $login, $idDashboard);
    }

    /**
     * Removes the dashboard with the given id
     */
    public function removeDashboard()
    {
        $this->checkTokenInUrl();

        if (Piwik::isUserIsAnonymous()) {
            return;
        }

        $idDashboard = Piwik_Common::getRequestVar('idDashboard', 1, 'int');

        // first layout can't be removed
        if ($idDashboard != 1) {
            $UserDashboard = Piwik_Db_Factory::getDAO('user_dashboard');
            $UserDashboard->deleteByLoginDashboard(
                Piwik::getCurrentUserLogin(),
                $idDashboard
            );
        }
    }

    /**
     * Outputs all available dashboards for the current user as a JSON string
     */
    public function getAllDashboards()
    {
        $this->checkTokenInUrl();
        if (Piwik::isUserIsAnonymous()) {
            echo '[]';
            return;
        }

        $login      = Piwik::getCurrentUserLogin();
        $dashboards = $this->dashboard->getAllDashboards($login);

        Piwik_DataTable_Renderer_Json::sendHeaderJSON();
        echo Piwik_Common::json_encode($dashboards);
    }

    /**
     * Creates a new dashboard for the current user
     * User needs to be logged in
     */
    public function createNewDashboard()
    {
        $this->checkTokenInUrl();

        if (Piwik::isUserIsAnonymous()) {
            echo '0';
            return;
        }
        $user = Piwik::getCurrentUserLogin();
        $UserDashboard = Piwik_Db_Factory::getDAO('user_dashboard');
        $nextId = $UserDashboard->getNextIdByLogin($user);

        $name = urldecode(Piwik_Common::getRequestVar('name', '', 'string'));
        $type = urldecode(Piwik_Common::getRequestVar('type', 'default', 'string'));
        $layout = '{}';

        if ($type == 'default') {
            $layout = $this->dashboard->getDefaultLayout();
        }

        $UserDashboard->newDashboard($user, $nextId, $name, $layout);
        echo Piwik_Common::json_encode($nextId);
    }

    public function copyDashboardToUser()
    {
        $this->checkTokenInUrl();

        if (!Piwik::isUserIsSuperUser()) {
            echo '0';
            return;
        }
        $login = Piwik::getCurrentUserLogin();
        $name = urldecode(Piwik_Common::getRequestVar('name', '', 'string'));
        $user = urldecode(Piwik_Common::getRequestVar('user', '', 'string'));
        $idDashboard = Piwik_Common::getRequestVar('dashboardId', 0, 'int');
        $layout = $this->dashboard->getLayoutForUser($login, $idDashboard);

        if ($layout !== false) {
            $UserDashboard = Piwik_Db_Factory::getDAO('user_dashboard');
            $nextId = $UserDashboard->getNextIdByLogin($user);

            $UserDashboard->newDashboard($user, $nextId, $name, $layout);

            Piwik_DataTable_Renderer_Json::sendHeaderJSON();
            echo Piwik_Common::json_encode($nextId);
            return;
        }
    }

    /**
     * Saves the layout for the current user
     * anonymous = in the session
     * authenticated user = in the DB
     */
    public function saveLayout()
    {
        $this->checkTokenInUrl();

        $layout = Piwik_Common::unsanitizeInputValue(Piwik_Common::getRequestVar('layout'));
        $idDashboard = Piwik_Common::getRequestVar('idDashboard', 1, 'int');
        $name = Piwik_Common::getRequestVar('name', '', 'string');
        if (Piwik::isUserIsAnonymous()) {
            $session = new Piwik_Session_Namespace("Piwik_Dashboard");
            $session->dashboardLayout = $layout;
            $session->setExpirationSeconds(1800);
        } else {
            $this->saveLayoutForUser(Piwik::getCurrentUserLogin(), $idDashboard, $layout);
            if (!empty($name)) {
                $this->updateDashboardName(Piwik::getCurrentUserLogin(), $idDashboard, $name);
            }
        }
    }

    /**
     * Saves the layout as default
     */
    public function saveLayoutAsDefault()
    {
        $this->checkTokenInUrl();

        if (Piwik::isUserIsSuperUser()) {
            $layout = Piwik_Common::unsanitizeInputValue(Piwik_Common::getRequestVar('layout'));
            $UserDashboard = Piwik_Db_Factory::getDAO('user_dashboard');
            $UserDashboard->saveLayout('', '1', $layout);
        }
    }

    /**
     * Get the dashboard layout for the current user (anonymous or logged user)
     *
     * @param int $idDashboard
     *
     * @return string $layout
     */
    protected function getLayout($idDashboard)
    {
        if (Piwik::isUserIsAnonymous()) {

            $session = new Piwik_Session_Namespace("Piwik_Dashboard");
            if (!isset($session->dashboardLayout)) {

                return $this->dashboard->getDefaultLayout();
            }

            $layout = $session->dashboardLayout;

        } else {
            $layout = $this->dashboard->getLayoutForUser(Piwik::getCurrentUserLogin(), $idDashboard);
        }

        if (!empty($layout)) {
            $layout = $this->dashboard->removeDisabledPluginFromLayout($layout);
        }

        if (empty($layout)) {
            $layout = $this->dashboard->getDefaultLayout();
        }

        return $layout;
    }

    /**
     * Returns all available column layouts for the dashboard
     *
     * @return array
     */
    protected function getAvailableLayouts()
    {
        return array(
            array(100),
            array(50, 50), array(67, 33), array(33, 67),
            array(33, 33, 33), array(40, 30, 30), array(30, 40, 30), array(30, 30, 40),
            array(25, 25, 25, 25)
        );
    }

}


