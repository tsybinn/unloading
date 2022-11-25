<?php

/**
 * Class Unloading
 */
class Unloading1249089
{
    /** @var PDO */
    public $pdo;
    /** @var string */
    public $resultFilePath;
    /** @var array */
    const SITES = [
        82 => 'ОП',
        83 => 'ОЗ',
        84 => 'БМ',
        85 => 'ГВ',
        86 => 'ОБ',
    ];
    /** @var array */
    private $arGoodsName;
    /** @var array */
    private $arResult;

    /**
     * Unloading1249089 constructor.
     */
    public function __construct()
    {
        set_time_limit(0);
        ini_set('memory_limit', '5000M');

        $DBHost = "";
        $DBName = "";
        $DBLogin = "";
        $DBPassword = "";

        $dsn = "mysql:host=$DBHost;dbname=$DBName";
        $pdo = new PDO($dsn, $DBLogin, $DBPassword);

        $this->pdo = $pdo;
        $this->resultFilePath = __DIR__ . '/result.csv';
    }

    public function run()
    {
        echo 'Start: ' . date('d.m.Y H:i:s') . PHP_EOL;
        //удаление файла с прошлой выгрузки
        if (file_exists($this->resultFilePath)) {
            unlink($this->resultFilePath);
            echo "The old file has been deleted" . PHP_EOL;
        }
        // добавляем заголовки столбцов
        $arHeaders = [
            'Логин клиента',
            'Наименование компании',
            'Город дилера',
            'Адрес сайта (ОЗ, ОП, ОБ, ГВ, БМ)',
            'Адрес собственного домена дилера (если есть)',
            'Дата последней загрузки своих цен',
            'Количество товаров на которые установлены свои цены',
            'Дата последней загрузки остатков на товары',
            'Количество загруженных остатков на товары',
            'Тип товарной матрицы (оптимальная, расширенная, максимальная)',
            'Тип минимальной партии (поштучная или рекомендованная)',
            'Количество СП',
            'Количество установленных конечным клиентам индивидуальных цен',
            'Количество созданных типов цен',
            'Индивидуальная фавиконка',
        ];
        $this->saveToFile($arHeaders);

        $this->getDealers();

        chmod($this->resultFilePath, 0666);
        echo 'Success' . PHP_EOL;
        echo 'Finish: ' . date('d.m.Y H:i:s') . PHP_EOL;

    }

    private function getSpecialProposals()
    {
        $sql = "SELECT UF_SHOP, count(ID) AS CNT
                    FROM hl_shops_price_condition
                    WHERE UF_ACTIVE = 1
                      AND UF_QUANTITY_TO = 1000000
                    GROUP BY UF_SHOP";

        $res = $this->pdo->query($sql);
        $arSpecialProposals = [];
        while ($arItem = $res->fetch(PDO::FETCH_ASSOC)) {
            $arSpecialProposals[$arItem['UF_SHOP']] = $arItem['CNT'];
        }

        return $arSpecialProposals;
    }

    private function getStocks()
    {
        $sql = "SELECT store.DATE_MODIFY              as `DATE_MODIFY`
           , COUNT(store_products.STORE_ID) as `COUNT`
           , shop.ID                        as `SHOP_ID`
      FROM hl_shops shop
               JOIN b_user_field_enum enum_site
                    ON shop.UF_SITE = enum_site.ID
               JOIN b_user dealer
                    ON dealer.ID = shop.UF_USER
               LEFT JOIN b_catalog_store store
                         ON store.XML_ID = shop.UF_USER
                             AND store.SHIPPING_CENTER = 'Y'
                             AND store.ISSUING_CENTER = 'N'
                             AND store.ACTIVE = 'Y'
               LEFT JOIN b_catalog_store_product store_products
                         ON store.ID = store_products.STORE_ID
      WHERE shop.UF_ACTIVE = 1
        AND dealer.ACTIVE = 'Y'
      GROUP BY shop.UF_USER, shop.UF_SITE
      ORDER BY dealer.LOGIN";

        $res = $this->pdo->query($sql);
        $arStocks = [];
        while ($arItem = $res->fetch(PDO::FETCH_ASSOC)) {
            $arStocks[$arItem['SHOP_ID']] = $arItem;
        }

        return $arStocks;
    }

    private function getDealers()
    {
        $arSpecialProposals = $this->getSpecialProposals();
        $arStocks = $this->getStocks();
        $arShopClientPrices = $this->getShopClientPrices();
        $arShopTypePrices = $this->getShopTypePrices();
        $arCustomFavicon = $this->getCustomFavicon();

        $sql = "
SELECT dealer.LOGIN, 
shop.ID as SHOP_ID,
shop.UF_NAME,
shop.UF_SITE,
shop.UF_PRICE_DEFAULT,
site.SERVER_NAME,
case
   when shop.UF_MATRIX = 'dks' then 'оптимальная'
   when shop.UF_MATRIX = 'opt' then 'максимальная'
   when shop.UF_MATRIX = 'ext' then 'расширенная'
   else 'оптимальная*'
end as UF_MATRIX,
case
   when shop.UF_MIN_PART_TYPE = 'kor' then 'рекомендованная'
   when shop.UF_MIN_PART_TYPE = 'single' then 'поштучная'
   else 'рекомендованная*'
end as UF_MIN_PART_TYPE,    
GROUP_CONCAT(DISTINCT region.NAME SEPARATOR ', ') as REGIONS  
FROM hl_shops shop  
JOIN b_user dealer ON dealer.ID = shop.UF_USER
LEFT JOIN b_lang site ON shop.UF_IDENTIFIER = site.NAME
LEFT JOIN hl_shops_delivery_region shop_regions ON shop_regions.UF_SHOP_ID = shop.ID
LEFT JOIN b_iblock_section region ON region.ID = shop_regions.UF_REGION AND region.IBLOCK_ID = 2
WHERE shop.UF_ACTIVE = 1
  AND dealer.ACTIVE = 'Y'
GROUP BY shop.UF_USER, shop.UF_SITE
ORDER BY dealer.LOGIN
                ";

        $res = $this->pdo->query($sql);
        while ($arItem = $res->fetch(PDO::FETCH_ASSOC)) {
            $arPrices = $this->getShopPrices($arItem['SHOP_ID']);

            $arDataToFile = [
                $arItem['LOGIN'],
                $arItem['UF_NAME'],
                $arItem['REGIONS'],
                self::SITES[$arItem['UF_SITE']],
                iconv('utf-8', 'cp1251', idn_to_utf8($arItem['SERVER_NAME'])),
                $arPrices['TIMESTAMP_X'],
                $arPrices['TOTAL_ITEMS'],
                $arStocks[$arItem['SHOP_ID']]['DATE_MODIFY'],
                $arStocks[$arItem['SHOP_ID']]['COUNT'],
                $arItem['UF_MATRIX'],
                $arItem['UF_MIN_PART_TYPE'],
                $arSpecialProposals[$arItem['SHOP_ID']],
                $arShopClientPrices[$arItem['SHOP_ID']] ?? 0,
                $arShopTypePrices[$arItem['SHOP_ID']] ?? 'Нет',
                $arCustomFavicon[$arItem['SHOP_ID']] ?? 'Нет',
            ];

            $this->saveToFile($arDataToFile);
        }
    }

    /**
     * @return array
     */
    private function getShopTypePrices(): array
    {
        $arShopTypePrices = [];

        $sql = "SELECT UF_SHOP_ID, count(UF_CLIENT_ID) AS CNT
                FROM hl_shops_price_type 
                WHERE UF_CLIENT_ID is not null
                GROUP BY UF_SHOP_ID";

        $res = $this->pdo->query($sql);
        while ($arItem = $res->fetch(PDO::FETCH_ASSOC)) {
            $arShopTypePrices[$arItem['UF_SHOP_ID']] = $arItem['CNT'];
        }

        return $arShopTypePrices;
    }

    /**
     * @return array
     */
    private function getShopClientPrices(): array
    {
        $arShopClientPrices = [];

        $sql = "SELECT UF_SHOP_ID, count(UF_CLIENT_ID) AS CNT
                FROM hl_shops_price_type 
                WHERE UF_CLIENT_ID is not null
                GROUP BY UF_SHOP_ID";

        $res = $this->pdo->query($sql);
        while ($arItem = $res->fetch(PDO::FETCH_ASSOC)) {
            $arShopClientPrices[$arItem['UF_SHOP_ID']] = $arItem['CNT'];
        }

        return $arShopClientPrices;
    }

    private function getShopPrices($shopId)
    {
        $sql = "
SELECT price_default.PRICE, price_default.PRODUCT_ID
FROM hl_shops shop
         LEFT JOIN b_catalog_group price_type_default ON shop.UF_PRICE_DEFAULT = price_type_default.NAME
         LEFT JOIN b_catalog_price price_default ON price_default.CATALOG_GROUP_ID = price_type_default.ID and (price_default.QUANTITY_FROM is null or price_default.QUANTITY_FROM = 0)  
         LEFT JOIN b_iblock_element bie ON bie.ID = price_default.PRODUCT_ID and bie.ACTIVE = 'Y' and bie.IBLOCK_ID = 9
WHERE shop.ID = {$shopId}
";

        $res = $this->pdo->query($sql);
        $arPricesDefault = [];
        if ($res) {
            while ($arItem = $res->fetch(PDO::FETCH_ASSOC)) {
                $arPricesDefault[$arItem['PRODUCT_ID']] = $arItem['PRICE'];
            }
        }

        $sql = "
SELECT price_type.TIMESTAMP_X, price.PRICE, price.PRODUCT_ID
FROM hl_shops shop
LEFT JOIN b_catalog_group price_type ON shop.UF_PRICE = price_type.NAME AND price_type.NAME LIKE 'Д\_%' 
LEFT JOIN b_catalog_price price ON price.CATALOG_GROUP_ID = price_type.ID and (price.QUANTITY_FROM is null or price.QUANTITY_FROM = 0)  
WHERE shop.ID = {$shopId}
";

        $res = $this->pdo->query($sql);
        $arPrices = [
            'TOTAL_ITEMS' => 0,
            'TIMESTAMP_X' => '',
        ];
        while ($arItem = $res->fetch(PDO::FETCH_ASSOC)) {
            if (empty($arPrices['TIMESTAMP_X'])) {
                $arPrices['TIMESTAMP_X'] = $arItem['TIMESTAMP_X'];
            }

            if ($arPricesDefault[$arItem['PRODUCT_ID']] != $arItem['PRICE']) {
                $arPrices['TOTAL_ITEMS']++;
            }
        }

        return $arPrices;
    }

    /**
     * Запись данных в файл
     *
     * @param array $arData
     * @param string $delimiter
     */
    private function saveToFile(array $arData, string $delimiter = ';')
    {
        if (!empty($arData)) {
            $string = '';
            foreach ($arData as $data) {
                $data = str_replace(chr(38), "", $data);
                $data = str_replace("quot;", "", $data);
                $data = str_replace('"', "", $data);
                $string .= nl2br($data) . $delimiter;
            }
            file_put_contents($this->resultFilePath, $string . PHP_EOL, FILE_APPEND);
        }
    }

    /**
     * кастомные фавиконки дилеров
     * @return array
     */
    private function getCustomFavicon(): array
    {
        $arCustomFavicon = [];
        foreach (new \DirectoryIterator(dirname(__FILE__) . '/favicon') as $obDirectory) {
            if ($obDirectory->isDir()) {
                $arCustomFavicon[] = iconv('utf-8', 'cp1251', idn_to_utf8($obDirectory->getBasename()));
            }
        }
        $matches = "'" . implode("','", $arCustomFavicon) . "'";

        $sql = "SELECT UF_SHOP, UF_DOMAIN FROM hl_site_domain WHERE UF_DOMAIN  IN($matches)";
        $res = $this->pdo->query($sql);
        $arFaviconDealer = [];
        while ($arItem = $res->fetch(PDO::FETCH_ASSOC)) {
            $arFaviconDealer[$arItem['UF_SHOP']] = 'Да';
        }

        return $arFaviconDealer;
    }
}

// Выгрузка
$unloading = new Unloading1249089();
$unloading->run();
