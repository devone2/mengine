{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
module Main where

import OrderBook
import Data.Hashable (Hashable)
import qualified Data.HashMap as HM
import qualified Data.Heap as H
import System.Random
import Control.Concurrent.MVar
import Control.Concurrent
import Data.Maybe
import Control.Monad
import System.Clock

makeRandomOrder id = do
  orderType <- randomIO :: IO Bool
  price <- randomRIO (if orderType then 40000 else 1, if orderType then 100000 else 60000)
  amount <- randomRIO (1, 10000000)
  return (Order (OrderId id) price amount (if orderType then Ask else Bid))
  
main :: IO ()
main = do
  putStrLn "Matching engine starting..."
  let clock = Monotonic
  book <- makeBookVar clock
  forkIO $ insertRandomOrders book
  periodicStatus book clock
  where periodicStatus book clock =
          let loop = do
                threadDelay (1000000 * 1)
                orderBook <- readMVar book
                time <- getTime clock
                putStrLn $ simpleStats orderBook time
                loop
          in loop

simpleStats book now =
  let stats = ordBookStats book
      start = stCreated stats
      orderRate = rate start now (stOrderCount stats)
      tradesRate = rate start now (stTradeCount stats)
  in
  "Best bid price: " ++ (show $ getBestBid book) ++ "\n"
  ++ "Best ask price: " ++ (show $ getBestAsk book) ++ "\n"
  ++ "Bid prices count: " ++ (show $ H.size $ ordBookBidLimits book) ++ "\n"
  ++ "Ask prices count: " ++ (show $ H.size $ ordBookAskLimits book) ++ "\n"
  ++ "Ask best orderId: " ++ (show $ getBestAskOrderId book) ++ "\n"
  ++ "Bid best orderId: " ++ (show $ getBestBidOrderId book) ++ "\n"
  ++ "Added order count: " ++ (show $ stOrderCount stats) ++ "\n"
  ++ "Added order rate: " ++ (show $ orderRate) ++ " per second \n"
  ++ "Executed trades count: " ++ (show $ stTradeCount $ ordBookStats book) ++ "\n"
  ++ "Executed trades rate: " ++ (show $ tradesRate) ++ " per second \n"
  where rate start end amount = let diff = diffTimeSpec start end
                                    damount = fromIntegral amount
                                    ddiff = fromIntegral $ sec diff
                                    in if (sec diff > 0) then Just $ damount / ddiff else Nothing
makeBookVar clock = do
  time <- getTime clock
  newMVar (emptyBook time)

addOrderVar book order = modifyMVar_ book (\b -> do
                                              let (book, mtrade) = execOrder $ addOrder b order
--                                              when (isJust mtrade) (putStrLn $ "Trade executed " ++ (show $ fromJust mtrade))
                                              return book)

insertRandomOrders book = do
--  orders <- mapM makeRandomOrder [1..100000000]
  mapM_ (\x -> do --threadDelay 1
                  order <- makeRandomOrder x  
                  (addOrderVar book order)) [1..10000000]  


testOrders = [ Order (OrderId 1) 100 1 Ask,
               Order (OrderId 2) 120 1 Bid
             ]
myTestBook = foldl (\b o -> addOrder b o) (emptyBook $ fromNanoSecs 0)  testOrders
  
