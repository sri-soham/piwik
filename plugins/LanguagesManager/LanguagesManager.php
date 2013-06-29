<?php
/**
 * Piwik - Open source web analytics
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 * @category Piwik_Plugins
 * @package Piwik_LanguagesManager
 *
 */

/**
 *
 * @package Piwik_LanguagesManager
 */
class Piwik_LanguagesManager extends Piwik_Plugin
{
    public function getInformation()
    {
        return array(
            'description'     => Piwik_Translate('LanguagesManager_PluginDescription'),
            'author'          => 'Piwik',
            'author_homepage' => 'http://piwik.org/',
            'version'         => Piwik_Version::VERSION,
        );
    }

    public function getListHooksRegistered()
    {
        return array(
            'AssetManager.getCssFiles'    => 'getCssFiles',
            'AssetManager.getJsFiles'     => 'getJsFiles',
            'TopMenu.add'                 => 'showLanguagesSelector',
            'Translate.getLanguageToLoad' => 'getLanguageToLoad',
            'UsersManager.deleteUser'     => 'deleteUserLanguage',
            'template_topBar'             => 'addLanguagesManagerToOtherTopBar',
        );
    }

    /**
     * @param Piwik_Event_Notification $notification  notification object
     */
    public function getCssFiles($notification)
    {
        $cssFiles = & $notification->getNotificationObject();

        $cssFiles[] = "themes/default/styles.css";
    }

    /**
     * @param Piwik_Event_Notification $notification  notification object
     */
    public function getJsFiles($notification)
    {
        $jsFiles = & $notification->getNotificationObject();

        $jsFiles[] = "plugins/LanguagesManager/templates/languageSelector.js";
    }

    /**
     * Show styled language selection drop-down list
     */
    public function showLanguagesSelector()
    {
        Piwik_AddTopMenu('LanguageSelector', $this->getLanguagesSelector(), true, $order = 30, true);
    }

    /**
     * Adds the languages drop-down list to topbars other than the main one rendered
     * in CoreHome/templates/top_bar.tpl. The 'other' topbars are on the Installation
     * and CoreUpdater screens.
     *
     * @param Piwik_Event_Notification $notification notification object
     */
    public function addLanguagesManagerToOtherTopBar($notification)
    {
        $str =& $notification->getNotificationObject();
        // piwik object & scripts aren't loaded in 'other' topbars
        $str .= "<script type='text/javascript'>if (!window.piwik) window.piwik={};</script>";
        $str .= "<script type='text/javascript' src='plugins/LanguagesManager/templates/languageSelector.js'></script>";
        $str .= $this->getLanguagesSelector();
    }

    /**
     * Renders and returns the language selector HTML.
     *
     * @return string
     */
    private function getLanguagesSelector()
    {
        // don't use Piwik_View::factory() here
        $view = new Piwik_View("LanguagesManager/templates/languages.tpl");
        $view->languages = Piwik_LanguagesManager_API::getInstance()->getAvailableLanguageNames();
        $view->currentLanguageCode = self::getLanguageCodeForCurrentUser();
        $view->currentLanguageName = self::getLanguageNameForCurrentUser();
        return $view->render();
    }

    /**
     * @param Piwik_Event_Notification $notification  notification object
     */
    public function getLanguageToLoad($notification)
    {
        $language =& $notification->getNotificationObject();
        if (empty($language)) {
            $language = self::getLanguageCodeForCurrentUser();
        }
        if (!Piwik_LanguagesManager_API::getInstance()->isLanguageAvailable($language)) {
            $language = Piwik_Translate::getInstance()->getLanguageDefault();
        }
    }

    /**
     * @param Piwik_Event_Notification $notification  notification object
     */
    public function deleteUserLanguage($notification)
    {
        $userLogin = $notification->getNotificationObject();
        $UserLanguage = Piwik_Db_Factory::getDAO('user_language');
        $UserLanguage->deleteByLogin($userLogin);
    }

    /**
     * @throws Exception if non-recoverable error
     */
    public function install()
    {
        $UserLanguage = Piwik_Db_Factory::getDAO('user_language');
        $UserLanguage->install();
    }

    /**
     * @throws Exception if non-recoverable error
     */
    public function uninstall()
    {
        $UserLanguage = Piwik_Db_Factory::getDAO('user_language');
        $UserLanguage->uninstall();
    }

    /**
     * @return string Two letters language code, eg. "fr"
     */
    public static function getLanguageCodeForCurrentUser()
    {
        $languageCode = self::getLanguageFromPreferences();
        if (!Piwik_LanguagesManager_API::getInstance()->isLanguageAvailable($languageCode)) {
            $languageCode = Piwik_Common::extractLanguageCodeFromBrowserLanguage(Piwik_Common::getBrowserLanguage(), Piwik_LanguagesManager_API::getInstance()->getAvailableLanguages());
        }
        if (!Piwik_LanguagesManager_API::getInstance()->isLanguageAvailable($languageCode)) {
            $languageCode = Piwik_Translate::getInstance()->getLanguageDefault();
        }
        return $languageCode;
    }

    /**
     * @return string Full english language string, eg. "French"
     */
    public static function getLanguageNameForCurrentUser()
    {
        $languageCode = self::getLanguageCodeForCurrentUser();
        $languages = Piwik_LanguagesManager_API::getInstance()->getAvailableLanguageNames();
        foreach ($languages as $language) {
            if ($language['code'] === $languageCode) {
                return $language['name'];
            }
        }
    }

    /**
     * @return string|false if language preference could not be loaded
     */
    protected static function getLanguageFromPreferences()
    {
        if (($language = self::getLanguageForSession()) != null) {
            return $language;
        }

        try {
            $currentUser = Piwik::getCurrentUserLogin();
            return Piwik_LanguagesManager_API::getInstance()->getLanguageForUser($currentUser);
        } catch (Exception $e) {
            return false;
        }
    }


    /**
     * Returns the langage for the session
     *
     * @return string|null
     */
    public static function getLanguageForSession()
    {
        $cookieName = Piwik_Config::getInstance()->General['language_cookie_name'];
        $cookie = new Piwik_Cookie($cookieName);
        if ($cookie->isCookieFound()) {
            return $cookie->get('language');
        }
        return null;
    }

    /**
     * Set the language for the session
     *
     * @param string $languageCode ISO language code
     * @return bool
     */
    public static function setLanguageForSession($languageCode)
    {
        if (!Piwik_LanguagesManager_API::getInstance()->isLanguageAvailable($languageCode)) {
            return false;
        }

        $cookieName = Piwik_Config::getInstance()->General['language_cookie_name'];
        $cookie = new Piwik_Cookie($cookieName, 0);
        $cookie->set('language', $languageCode);
        $cookie->save();
    }
}
