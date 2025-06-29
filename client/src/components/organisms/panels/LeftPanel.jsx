import React, { useEffect, useState } from "react";
import { Accordion } from "@/components/ui/accordion";
import ListItem from "../../atoms/ListItem";
import AccordionPanelItem from "../../molecules/AccordionPanelItem";
import { useErrorHandler } from "@/hooks/useErrorHandler";
import { sportsAdapter } from "@/services/api";

const LeftPanel = () => {
  const [popular, setPopular] = useState([]);
  const [sports, setSports] = useState([]);
  const { handleError } = useErrorHandler();

  useEffect(() => {
    const handleFetchData = async () => {
      try {
        const popularLeagues = await sportsAdapter.getPopularLeagues();
        const sportsWithLeagues = await sportsAdapter.getSportsWithLeagues();
        setPopular(popularLeagues);
        setSports(sportsWithLeagues);
      } catch (err) {
        handleError(err);
      }
    };
    handleFetchData();
  }, []);

  return (
    <div className="max-w-[350px] mx-auto">
      <div className="space-y-6">
        <section>
          <h2 className="text-lg font-semibold mb-3 text-gray-800">Popular</h2>
          <div className="bg-white rounded-lg shadow-sm min-h-[50px]">
            <div className="divide-y divide-gray-100">
              {Array.isArray(popular) && popular.length > 0 ? (
                popular.map((league) => (
                  <ListItem key={league.id} country={league.country_code} to={league.url_path}>
                    {league.name}
                  </ListItem>
                ))
              ) : (
                <div className="w-full h-[50px] flex items-center justify-center">No active popular league.</div>
              )}
            </div>
          </div>
        </section>

        <section>
          <h2 className="text-lg font-semibold mb-3 text-gray-800">Sport</h2>
          <div className="bg-white rounded-lg shadow-sm min-h-[50px]">
            <Accordion type="single" collapsible className="w-full">
              {Array.isArray(sports) && sports.length > 0 ? (
                sports.map((sport) => (
                  <AccordionPanelItem key={sport.id} title={sport.name}>
                    {sport.leagues.map((league, i) => (
                      <ListItem key={i} country={league.country_code} to={league.url_path}>
                        {league.name}
                      </ListItem>
                    ))}
                  </AccordionPanelItem>
                ))
              ) : (
                <div className="w-full h-[50px] flex items-center justify-center">No active sport.</div>
              )}
            </Accordion>
          </div>
        </section>
      </div>
    </div>
  );
};

export default LeftPanel;
